// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

use std::{fs::File, io};

/// An application's exclusive journal ownership, independent of descriptors inherited
/// across fork before a child's close-on-exec takes effect.
pub struct ExclusiveFileLock {
    file: File,
    ownership: Ownership,
}

enum Ownership {
    Held,
    Released,
}

impl ExclusiveFileLock {
    /// Acquire the open file description without waiting for another owner.
    pub fn acquire(file: File) -> io::Result<Self> {
        file.try_lock().map_err(io::Error::from)?;
        Ok(Self {
            file,
            ownership: Ownership::Held,
        })
    }

    /// Explicitly unlock before reporting a successful terminal operation.
    pub(crate) fn release(&mut self) -> io::Result<()> {
        if matches!(self.ownership, Ownership::Held) {
            self.file.unlock()?;
            self.ownership = Ownership::Released;
        }
        Ok(())
    }
}

impl AsRef<File> for ExclusiveFileLock {
    fn as_ref(&self) -> &File {
        &self.file
    }
}

impl Drop for ExclusiveFileLock {
    fn drop(&mut self) {
        // The final owner has stopped conducting journal transactions. Drop cannot return a
        // failure and the File still closes normally if the kernel refuses the explicit release.
        if let Err(error) = self.release() {
            use std::io::Write;
            let _ = writeln!(
                io::stderr().lock(),
                "sparrow: releasing journal ownership failed: {error}"
            );
        }
    }
}
