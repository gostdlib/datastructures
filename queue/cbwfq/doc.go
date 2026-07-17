// Package cbwfq provides a Class-Based Weighted Fair Queuing scheduler over a set
// of queue.Queue instances, one queue per traffic class. It classifies items into
// classes on Push and schedules dequeues across classes on Pull so that dequeue
// bandwidth is shared in proportion to each class's weight.
//
// The scheduler is the software equivalent of a router's LLQ + CBWFQ:
//
//   - Classify on Push: a caller-supplied Classifier maps each item to a class
//     name; the item is routed into that class's queue. An item that names no
//     known class is routed to the mandatory Default class.
//   - Schedule on Pull: the optional strict-priority (Priority) class is drained
//     first whenever it is non-empty (like router network-control traffic);
//     otherwise a Deficit Weighted Round Robin picks the next class, granting each
//     class a per-round quantum equal to its Weight. Every item is unit cost, so a
//     weight-3 class is served three items for every one item of a weight-1 class.
//     Scheduling is work-conserving: an empty class is skipped, never idled.
//
// A class set must contain exactly one Default class and at most one Priority
// class. Weight is a property of the class, not the item. Construct the per-class
// queues with the queue package, hand them to New, and thereafter mutate them only
// through the scheduler.
//
// Pull blocks until some class has an item (or the context is canceled); a batch
// Pull returns items from a single class per call. Push blocks only if the target
// class is a bounded queue that is full. Both honor context cancellation and
// return queue.ErrClosed after Close.
package cbwfq
