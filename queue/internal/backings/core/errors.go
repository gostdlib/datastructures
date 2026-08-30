package core

import (
	"errors"
	"fmt"

	"github.com/gostdlib/base/retry/exponential"
)

// permanent tags a sentinel as non-retryable, so a caller that wraps a queue operation in
// github.com/gostdlib/base/retry/exponential — the house pattern for a durable write — stops on it
// instead of spinning on a programmer error until the retry budget runs out. Every sentinel that
// reports a mistake no amount of waiting can fix is built through this; the two that report a
// transient state a retry can legitimately clear (ErrEmpty and ErrShutdownIncomplete) deliberately
// are not.
//
// It is a wrapper rather than a second sentinel joined at each site because the value the package
// exports has to be the permanent one: a caller matches errors.Is(err, ErrClosed) against exactly
// this value, and marking it anywhere else would leave errors.Is(err, exponential.ErrPermanent)
// false for every error the package actually returns.
func permanent(msg string) error {
	return fmt.Errorf("%s: %w", msg, exponential.ErrPermanent)
}

var (
	// ErrEmpty is returned when the queue is empty and there are no items to pop. Deliberately not
	// permanent: an empty queue is a state a retry can clear.
	ErrEmpty = errors.New("queue is empty")
	// ErrClosed is returned when an operation is attempted on a closed queue.
	ErrClosed = permanent("queue is closed")
	// ErrBadOption tags every error a misused construction or per-operation option produces: a
	// value outside the range the option documents, an option handed to a constructor or operation
	// it is not valid for, and a required option that was not supplied. It is one sentinel rather
	// than one per option because there is nothing a caller can do differently for each — they are
	// all "this call site is configured wrong, fix the code" — and because the alternative had
	// already gone wrong in practice: "max batch must be at least 1" existed as six unrelated
	// errors.New values across six backings, six competing identities for one semantic error that
	// no errors.Is could unify. The specific message stays in the wrapped error.
	//
	// The named option sentinels below (ErrCodecRequired, ErrOnAdmitUnsupported, ErrOnAdmitNotPush,
	// ErrNilOnAdmit, ErrNilSideEffect) are built on top of this, so errors.Is(err, ErrBadOption) is
	// the single question "did I configure this wrong?" and each of those remains the finer answer.
	ErrBadOption = permanent("invalid option")
	// ErrBatchTooLarge is returned by Push when the batch exceeds the configured max
	// batch size (WithMaxBatch) or a bounded queue's maximum size.
	ErrBatchTooLarge = permanent("batch larger than allowed maximum")
	// ErrPriorityRequired is returned when an item with Priority() == 0 is pushed onto a priority queue.
	ErrPriorityRequired = permanent("priority queue requires items with Priority() > 0")
	// ErrPriorityNotAllowed is returned when an item with Priority() > 0 is pushed onto a FIFO queue.
	ErrPriorityNotAllowed = permanent("FIFO queue requires items with Priority() == 0")
	// ErrCodecRequired is returned when a Value without both Encoder and Decoder set is
	// used with an on-disk (bbolt) backing.
	ErrCodecRequired = fmt.Errorf("%w: on-disk queue requires Value.Encoder and Value.Decoder to be set", ErrBadOption)
	// ErrOnAdmitUnsupported is returned when WithOnAdmit is used on a backing that cannot run a
	// hook between admission and insertion. Only the on-disk backing is in this position: it
	// admits into a staging buffer and commits in groups, so there is no point in its Push that
	// corresponds to "capacity is reserved and nothing is written yet".
	ErrOnAdmitUnsupported = fmt.Errorf("%w: this backing does not support WithOnAdmit", ErrBadOption)
	// ErrOnAdmitNotPush is returned when WithOnAdmit is given to an operation other than Push.
	// Push is the only operation with an admission step for the hook to sit in, so anywhere else
	// the hook could never run — and it carries the caller's durable write, which makes silently
	// accepting it the wrong answer.
	ErrOnAdmitNotPush = fmt.Errorf("%w: WithOnAdmit is only valid for Push", ErrBadOption)
	// ErrNilOnAdmit is returned by WithOnAdmit when handed a nil func.
	ErrNilOnAdmit = fmt.Errorf("%w: WithOnAdmit requires a non-nil func", ErrBadOption)
	// ErrNilSideEffect is returned by WithSideEffect when handed a nil func.
	ErrNilSideEffect = fmt.Errorf("%w: WithSideEffect requires a non-nil func", ErrBadOption)
	// ErrOnAdmitFailed wraps whatever a WithOnAdmit hook returned, so a caller can tell its own
	// hook's failure from one the queue produced. Without it a hook that happens to return, say,
	// ErrClosed makes Push report ErrClosed on a queue that is perfectly open. The hook's error
	// stays reachable through errors.Is/As.
	ErrOnAdmitFailed = errors.New("the WithOnAdmit hook failed")
	// ErrBackupFailed reports that the caller's Backup failed. It tags every error a Backup method
	// returns, and additionally the two ways a Backup can end an operation without returning — a
	// panic or a runtime.Goexit — on the on-disk backing's flusher, which is not the caller's
	// goroutine to lose. Everywhere else such an unwind continues on the caller's own goroutine
	// and the queue only guarantees it does not take the lock with it.
	//
	// The tag is additive, so a Backup that returns one of this package's own sentinels still
	// matches it: errors.Is(err, ErrClosed) can be true for an error the Backup produced. Test the
	// five caller-code sentinels — ErrBackupFailed, ErrCodecFailed, ErrSideEffectFailed,
	// ErrOnAdmitFailed and ErrItemFailed — first; only when all five are false does a queue
	// sentinel mean the queue itself.
	//
	// One case does not fit that reading, and it is the reason this paragraph exists. Pop and Del
	// mirror the removal to the Backup before applying it on disk; when the on-disk delete then
	// fails, the queue calls Backup.Restore to put the mirror back. If that compensating Restore
	// also fails, both errors are joined and returned — so the tag is present for a failure the
	// QUEUE caused, and the check order above would have the caller blame their Backup for it. The
	// joined error carries the queue's own failure alongside, and the situation it describes is
	// worse than either: the delete did not land, the restore did not either, and the Backup is
	// genuinely out of sync with the queue. Treat ErrBackupFailed as "the Backup was involved",
	// and on Pop and Del read the whole joined error rather than only the tag.
	ErrBackupFailed = errors.New("the Backup implementation failed")
	// ErrShutdownIncomplete is returned by Close when the backing could not be released. There are
	// three causes, and they do not all mean the same thing:
	//
	//   - The flusher would not stop within the join deadline. Nothing was released — the db is
	//     deliberately not closed under a live flusher — so the bolt file lock is still held and a
	//     later Close retries.
	//   - Another Close holding the release job did not finish within this caller's deadline, and
	//     had not yet committed to releasing. Same meaning as above: nothing released, retry.
	//   - Releasing the bolt handle itself failed (db.Close returned an error). Unlike the other
	//     two this one is terminal: the release is already committed, so no later Close re-runs it,
	//     and the caller cannot tell from bbolt's own error whether the file lock is still held.
	//     Treat the store as possibly still locked by this process and do not reopen it in place.
	//
	// It is deliberately an error rather than silence: the previous designs returned nil here, and
	// a caller cannot act on a shutdown that only looked successful. It is deliberately NOT marked
	// permanent, because the first two causes are exactly what a retry is for.
	ErrShutdownIncomplete = errors.New("the backing could not be shut down")
	// ErrShutdownInProgress is returned to a Close that gave up waiting for another Close that had
	// already committed to releasing. Unlike ErrShutdownIncomplete it does not mean "nothing
	// happened, retry": the release is under way and no retry will re-run it.
	ErrShutdownInProgress = permanent("a shutdown of the backing is already in progress")
	// ErrCodecFailed reports that the item codec failed. It tags every error the codec returns —
	// the WithCodec funcs, or the default JSON encoding when none was supplied — and the two ways
	// the codec can end an operation without returning on the on-disk backing's flusher. It is
	// separate from ErrBackupFailed because they are different pieces of caller code: a queue with
	// no Backup at all can still have a codec that fails.
	ErrCodecFailed = errors.New("the item codec failed")
	// ErrSideEffectFailed wraps whatever a WithSideEffect func returned, for the same reason, and
	// also carries the two ways a side effect can end without returning on the one backing where
	// that is not the caller's own goroutine to lose. See RunSideEffectLocked and bbolt's Clear.
	ErrSideEffectFailed = errors.New("the WithSideEffect func failed")
	// ErrItemFailed reports that one of the caller's Item methods failed. It is the fifth body of
	// caller code, and the one nobody opts into: the Item constraint requires Less, Equal,
	// Priority and Hash, so every queue runs them even when it has no Backup, no codec, no side
	// effect and no hook. Without this sentinel an Item that panicked on the flusher was reported
	// as "the queue panicked while committing", with no sentinel at all — the queue taking the
	// blame for the caller's code, which is exactly what the other four sentinels exist to prevent.
	//
	// Item methods return no error, so there is never a returned error to tag: this carries only
	// the two ways one of them can end an operation without returning on the on-disk backing's
	// flusher, which is not the caller's goroutine to lose. The flusher's commit path calls Hash
	// (for WithIndex) and Priority (through the priority key), so those are the two methods that
	// can produce it. Less and Equal are never reached on the flusher.
	//
	// Everywhere else — Less and Equal always, Hash and Priority on Push, Pop, Del and Exists, and
	// Hash again on the New caller's goroutine, where rebuildIndex calls it for every record in an
	// existing on-disk store and Hydrate calls it for every item loaded from a Backup — the method
	// runs on the caller's own goroutine, and a panic or runtime.Goexit there keeps
	// unwinding on that goroutine with its own stack, carrying no sentinel. That is deliberate and
	// matches every other body of caller code (see ErrBackupFailed): the queue's only guarantee for
	// an unwind on the caller's goroutine is that it does not take a queue lock with it. Converting
	// it into an error here would mean recovering a panic the caller owns and is entitled to catch.
	ErrItemFailed = errors.New("the Item implementation failed")
)

// WrapBackup tags an error a Backup method returned, so a caller can tell its own Backup's failure
// from one the queue produced — the same discriminator ErrSideEffectFailed and ErrOnAdmitFailed
// give the other two bodies of caller code.
//
// It adds a discriminator; it does not remove the ambiguity. The Backup's error stays reachable, so
// a Backup returning ErrClosed still matches errors.Is(err, ErrClosed) — what changes is that
// ErrBackupFailed is there to be asked about first. See ErrBackupFailed for the check order.
func WrapBackup(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", ErrBackupFailed, err)
}

// WrapCodec tags an error the caller's item codec returned, the same way WrapBackup tags a Backup's.
func WrapCodec(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", ErrCodecFailed, err)
}
