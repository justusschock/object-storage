package objectstore

import (
	"io"
	"sync/atomic"
)

// ProgressFunc observes transfer progress. transferred is the cumulative byte
// count and total is the expected byte count, or -1 when unknown. Callbacks
// run synchronously on the reading goroutine and must be fast.
type ProgressFunc func(transferred, total int64)

// WithProgress wraps reader so that every successful Read reports cumulative
// progress. Wrap a Put body to observe uploads (pass PutOptions.Size, because
// the wrapper hides the reader's own length) or a Get body to observe
// downloads. A nil progress function returns reader unchanged.
func WithProgress(reader io.Reader, total int64, progress ProgressFunc) io.Reader {
	if progress == nil {
		return reader
	}
	return &progressReader{reader: reader, total: total, progress: progress}
}

// WithProgressReadCloser is WithProgress for bodies that must be closed, such
// as the reader returned by Store.Get.
func WithProgressReadCloser(body io.ReadCloser, total int64, progress ProgressFunc) io.ReadCloser {
	if progress == nil {
		return body
	}
	return progressReadCloser{progressReader: &progressReader{reader: body, total: total, progress: progress}, closer: body}
}

type progressReader struct {
	reader      io.Reader
	total       int64
	transferred atomic.Int64
	progress    ProgressFunc
}

func (reader *progressReader) Read(buffer []byte) (int, error) {
	count, err := reader.reader.Read(buffer)
	if count > 0 {
		reader.progress(reader.transferred.Add(int64(count)), reader.total)
	}
	return count, err
}

type progressReadCloser struct {
	*progressReader
	closer io.Closer
}

func (reader progressReadCloser) Close() error {
	return reader.closer.Close()
}
