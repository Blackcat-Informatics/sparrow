// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Writers ahead of online migration pages (#146).
//!
//! The catalog writer is a `std::sync::Mutex`, which is not fair: a migration page that
//! releases it and asks again at once can win it back from a writer already waiting. Every
//! ordinary writer registers here while it waits for the writer lock, and before a page the
//! maintenance worker waits, apart from its other duties, until no writer is waiting. The
//! wait is bounded so the migration still progresses under a continuous stream of writers:
//! a page defers at most the longer of [`MIN_DEFERRAL`] and the time its predecessor held
//! the writer. The wait blocks on a condition variable that the last waiting writer signals.
//! The archive's selection batches and record reads defer the same way before each page.

use super::Error;
use std::sync::{Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

/// Shortest bound on one page's deferral.
const MIN_DEFERRAL: Duration = Duration::from_millis(100);

#[derive(Default)]
struct State {
    /// Writers waiting for the writer lock.
    waiting: usize,
    /// How long the last migration page held the writer.
    last_page: Duration,
}

#[derive(Default)]
pub(super) struct WriterQueue {
    state: Mutex<State>,
    drained: Condvar,
}

/// A writer's registration; dropping it once the writer holds the lock withdraws it.
pub(super) struct Arrival<'q> {
    queue: &'q WriterQueue,
}

impl Drop for Arrival<'_> {
    fn drop(&mut self) {
        // A poisoned registry only arises from a panic while it was held; the writer lock
        // itself reports that failure, so the count is still withdrawn.
        let mut state = self.queue.state();
        state.waiting = state.waiting.saturating_sub(1);
        if state.waiting == 0 {
            self.queue.drained.notify_all();
        }
    }
}

impl WriterQueue {
    /// The registry. A poisoned registry only arises from a panic while it was held, which
    /// leaves its counts consistent, so it is recovered rather than refused.
    fn state(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Register a writer about to wait for the writer lock.
    pub(super) fn arrive(&self) -> Result<Arrival<'_>, Error> {
        let mut state = self.state();
        state.waiting = state
            .waiting
            .checked_add(1)
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "writer queue count overflow"))?;
        Ok(Arrival { queue: self })
    }

    /// Wait until no writer is waiting, at most the page's deferral bound; returns how long
    /// the page deferred.
    pub(super) fn defer_page(&self) -> Duration {
        let started = Instant::now();
        let mut state = self.state();
        let bound = state.last_page.max(MIN_DEFERRAL);
        while state.waiting > 0 {
            let Some(remaining) = bound.checked_sub(started.elapsed()) else {
                break;
            };
            state = self
                .drained
                .wait_timeout(state, remaining)
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .0;
        }
        started.elapsed()
    }

    /// Record how long a page held the writer, which bounds the next page's deferral.
    pub(super) fn page_held(&self, held: Duration) {
        self.state().last_page = held;
    }
}

/// Writer state that `status` observes without the writer lock (#146): durability,
/// pressure and table rewrite progress. The writer keeps the working state to itself, as it
/// did in a `RefCell`, and may hold it across commits; when a mutable borrow ends it
/// publishes a small [`Observe::View`] copy. Status reads only that copy, so it waits at
/// most for one short copy, never for a pressure pass, a rewrite step or a commit.
pub(super) trait Observe {
    /// What status reads.
    type View: Clone;
    /// The view of the current state.
    fn view(&self) -> Self::View;
}

pub(super) struct Observed<T: Observe> {
    local: std::cell::RefCell<T>,
    published: Published<T::View>,
}

/// The published view, shared with the [`super::Store`] handles.
pub(super) struct Published<V>(std::sync::Arc<std::sync::RwLock<V>>);

impl<V> Clone for Published<V> {
    fn clone(&self) -> Self {
        Self(std::sync::Arc::clone(&self.0))
    }
}

impl<V: Clone> Published<V> {
    /// A copy of the last published view. A poisoned lock is recovered: a panic while the
    /// writer published already poisoned the writer lock, which refuses every later write.
    pub(super) fn read(&self) -> V {
        self.0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl<T: Observe> Observed<T> {
    pub(super) fn new(value: T) -> Self {
        Self {
            published: Published(std::sync::Arc::new(std::sync::RwLock::new(value.view()))),
            local: std::cell::RefCell::new(value),
        }
    }

    /// The handle status reads.
    pub(super) fn published(&self) -> Published<T::View> {
        self.published.clone()
    }

    /// Read the working state; only the writer-lock holder calls this.
    pub(super) fn borrow(&self) -> std::cell::Ref<'_, T> {
        self.local.borrow()
    }

    /// Change the working state; the view is published when the borrow ends.
    pub(super) fn borrow_mut(&self) -> Publishing<'_, T> {
        Publishing {
            local: self.local.borrow_mut(),
            published: &self.published,
        }
    }
}

/// A mutable borrow that publishes the view when it ends.
pub(super) struct Publishing<'a, T: Observe> {
    local: std::cell::RefMut<'a, T>,
    published: &'a Published<T::View>,
}

impl<T: Observe> std::ops::Deref for Publishing<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.local
    }
}

impl<T: Observe> std::ops::DerefMut for Publishing<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.local
    }
}

impl<T: Observe> Drop for Publishing<'_, T> {
    fn drop(&mut self) {
        let view = self.local.view();
        *self
            .published
            .0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = view;
    }
}
