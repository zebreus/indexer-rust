// use crate::stream::{Fuse, FuturesUnordered, StreamExt};
use core::fmt;
use futures::stream::FuturesUnordered;
use futures::{Sink, Stream, StreamExt};
use std::pin::Pin;
use std::task::Poll;
// use futures_core::task::{Context, Poll};
use pin_project_lite::pin_project;
use std::future::Future;
// use std::stream::{FusedStream, Stream};

// pin_project! {
/// Stream for the [`buffer_unordered`](super::StreamExt::buffer_unordered)
/// method.
#[must_use = "streams do nothing unless polled"]
pub struct BufferItems<St>
where
    St: Stream,
{
    stream: St,
    in_progress_queue: FuturesUnordered<St::Item>,
    max: usize,
}
// }

impl<St> fmt::Debug for BufferItems<St>
where
    St: Stream + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BufferItems")
            .field("stream", &self.stream)
            .field("in_progress_queue", &self.in_progress_queue)
            .field("max", &self.max)
            .finish()
    }
}

impl<St> BufferItems<St>
where
    St: Stream,
    St::Item: Future,
{
    pub fn new(stream: St, n: usize) -> Self {
        Self {
            stream: stream,
            in_progress_queue: FuturesUnordered::new(),
            max: n,
        }
    }

    // delegate_access_inner!(stream, St, (.));
}

impl<St> Stream for BufferItems<St>
where
    St: Stream,
    St::Item: Future,
{
    type Item = <St::Item as Future>::Output;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut this = self;

        // First up, try to spawn off as many futures as possible by filling up
        // our queue of futures.
        while this.in_progress_queue.len() < *this.max {
            match this.stream.poll_next_unpin(cx) {
                Poll::Ready(Some(fut)) => this.in_progress_queue.push(fut),
                Poll::Ready(None) | Poll::Pending => break,
            }
        }

        // Attempt to pull the next value from the in_progress_queue
        match this.in_progress_queue.poll_next_unpin(cx) {
            x @ Poll::Pending | x @ Poll::Ready(Some(_)) => return x,
            Poll::Ready(None) => {}
        }

        // If more values are still coming from the stream, we're not done yet
        if this.stream.is_done() {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let queue_len = self.in_progress_queue.len();
        let (lower, upper) = self.stream.size_hint();
        let lower = lower.saturating_add(queue_len);
        let upper = match upper {
            Some(x) => x.checked_add(queue_len),
            None => None,
        };
        (lower, upper)
    }
}

// impl<St> FusedStream for BufferItems<St>
// where
//     St: Stream,
//     St::Item: Future,
// {
//     fn is_terminated(&self) -> bool {
//         self.in_progress_queue.is_terminated() && self.stream.is_terminated()
//     }
// }

// impl<St> Stream for BufferItems<St>
// where
//     St: Stream,
//     St::Item: Future,
// {
//     type Item = <St::Item as Future>::Output;

//     fn poll_next(
//         mut self: std::pin::Pin<&mut Self>,
//         cx: &mut std::task::Context<'_>,
//     ) -> Poll<Option<Self::Item>> {
//         let mut this = self.project();

//         // First up, try to spawn off as many futures as possible by filling up
//         // our queue of futures.
//         while this.in_progress_queue.len() < *this.max {
//             match this.stream.as_mut().poll_next(cx) {
//                 Poll::Ready(Some(fut)) => this.in_progress_queue.push(fut),
//                 Poll::Ready(None) | Poll::Pending => break,
//             }
//         }

//         // Attempt to pull the next value from the in_progress_queue
//         match this.in_progress_queue.poll_next_unpin(cx) {
//             x @ Poll::Pending | x @ Poll::Ready(Some(_)) => return x,
//             Poll::Ready(None) => {}
//         }

//         // If more values are still coming from the stream, we're not done yet
//         if this.stream.is_done() {
//             Poll::Ready(None)
//         } else {
//             Poll::Pending
//         }
//     }

//     fn size_hint(&self) -> (usize, Option<usize>) {
//         let queue_len = self.in_progress_queue.len();
//         let (lower, upper) = self.stream.size_hint();
//         let lower = lower.saturating_add(queue_len);
//         let upper = match upper {
//             Some(x) => x.checked_add(queue_len),
//             None => None,
//         };
//         (lower, upper)
//     }
// }

// Forwarding impl of Sink from the underlying stream
// impl<S, Item> Sink<Item> for BufferItems<S>
// where
//     S: Stream + Sink<Item>,
//     S::Item: Future,
// {
//     type Error = S::Error;

//     fn poll_ready(
//         self: Pin<&mut Self>,
//         cx: &mut core::task::Context<'_>,
//     ) -> core::task::Poll<Result<(), Self::Error>> {
//         self.project().stream.poll_ready(cx)
//     }
//     fn start_send(self: Pin<&mut Self>, item: Item) -> Result<(), Self::Error> {
//         self.project().stream.start_send(item)
//     }
//     fn poll_flush(
//         self: Pin<&mut Self>,
//         cx: &mut core::task::Context<'_>,
//     ) -> core::task::Poll<Result<(), Self::Error>> {
//         self.project().stream.poll_flush(cx)
//     }
//     fn poll_close(
//         self: Pin<&mut Self>,
//         cx: &mut core::task::Context<'_>,
//     ) -> core::task::Poll<Result<(), Self::Error>> {
//         self.project().stream.poll_close(cx)
//     }
// }
