package utils

import (
	"EverythingSuckz/fsb/internal/types"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/celestix/gotgproto"
	"go.uber.org/zap"
)

// multiPartTelegramReader implements io.ReadCloser for streaming from multiple parts.
type multiPartTelegramReader struct {
	ctx        context.Context
	cancel     context.CancelFunc // To stop the worker
	client     *gotgproto.Client
	manifest   *types.Manifest
	start      int64 // Global start offset
	end        int64 // Global end offset
	contentLength int64
	
	pipeReader *io.PipeReader // We read from this
	pipeWriter *io.PipeWriter // The worker writes to this
	log        *zap.Logger

	// Internal state for the worker
	partFileCache map[int]*types.File // Caches metadata of parts
	cacheMu    sync.Mutex
	workerErr  error // Stores any error from the worker
}

// NewMultiPartTelegramReader creates a new reader for a multi-part file.
func NewMultiPartTelegramReader(
	ctx context.Context,
	client *gotgproto.Client,
	manifest *types.Manifest,
	start int64,
	end int64,
	contentLength int64,
) (io.ReadCloser, error) {
	
	pr, pw := io.Pipe()
	ctx, cancel := context.WithCancel(ctx)

	r := &multiPartTelegramReader{
		ctx:           ctx,
		cancel:        cancel,
		client:        client,
		manifest:      manifest,
		start:         start,
		end:           end,
		contentLength: contentLength,
		pipeReader:    pr,
		pipeWriter:    pw,
		log:           Logger.Named("multiPartReader"),
		partFileCache: make(map[int]*types.File),
	}

	// Start the background worker goroutine
	go r.runWorker()

	return r, nil
}

// Read implements io.Reader. It reads from the pipe.
func (r *multiPartTelegramReader) Read(p []byte) (n int, err error) {
	return r.pipeReader.Read(p)
}

// Close implements io.Closer. It cancels the context and closes the pipes.
func (r *multiPartTelegramReader) Close() error {
	r.cancel() // Signal worker to stop
	r.pipeReader.Close()
	r.pipeWriter.Close()
	return nil
}

// getPartFile fetches and caches the metadata for a part (message).
func (r *multiPartTelegramReader) getPartFile(ctx context.Context, messageID int) (*types.File, error) {
	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()

	if file, ok := r.partFileCache[messageID]; ok {
		return file, nil
	}
	
	// Use the existing FileFromMessage, which has its own caching
	file, err := FileFromMessage(ctx, r.client, messageID)
	if err != nil {
		return nil, fmt.Errorf("could not get file info for part msg %d: %w", messageID, err)
	}

	if file.FileSize == 0 {
		// This likely means it's a photo, which we need to handle by getting its size.
		// For simplicity, we'll assume parts are not photos.
		// If they can be, this logic needs to be expanded to get photo size.
		r.log.Warn("Part file has zero size, streaming may fail", zap.Int("msg_id", messageID))
	}

	r.partFileCache[messageID] = file
	return file, nil
}

// findPartForOffset finds which part a global offset belongs to.
func (r *multiPartTelegramReader) findPartForOffset(globalOffset int64) (partIndex int, localOffset int64, err error) {
	var currentOffset int64 = 0
	for i, partMessageID := range r.manifest.Parts {
		partFile, err := r.getPartFile(r.ctx, partMessageID)
		if err != nil {
			return 0, 0, fmt.Errorf("pre-flight check failed for part %d: %w", partMessageID, err)
		}

		if currentOffset+partFile.FileSize > globalOffset {
			// The offset is inside this part
			localOffset := globalOffset - currentOffset
			return i, localOffset, nil
		}
		currentOffset += partFile.FileSize
	}
	
	// This can happen if start offset is exactly TotalSize
	if globalOffset == r.manifest.TotalSize {
		return len(r.manifest.Parts), 0, nil
	}

	return 0, 0, fmt.Errorf("offset %d out of bounds (total_size: %d)", globalOffset, r.manifest.TotalSize)
}

// runWorker is the background goroutine that fetches parts and writes to the pipe.
func (r *multiPartTelegramReader) runWorker() {
	// When worker exits, close the writer. This will send io.EOF to the reader.
	defer r.pipeWriter.Close()

	var bytesWritten int64 = 0

	// 1. Find the starting part and the local offset within that part
	partIndex, localOffset, err := r.findPartForOffset(r.start)
	if err != nil {
		r.pipeWriter.CloseWithError(fmt.Errorf("could not find start offset: %w", err))
		return
	}

	// 2. Loop through the parts, starting from the one we found
	for partIndex < len(r.manifest.Parts) && bytesWritten < r.contentLength {
		// Check if context was cancelled
		select {
		case <-r.ctx.Done():
			r.pipeWriter.CloseWithError(context.Canceled)
			return
		default:
		}
		
		partMessageID := r.manifest.Parts[partIndex]
		partFile, err := r.getPartFile(r.ctx, partMessageID)
		if err != nil {
			r.pipeWriter.CloseWithError(err)
			return
		}

		// 3. Calculate how much to read from this *specific* part
		bytesRemainingInRequest := r.contentLength - bytesWritten
		bytesRemainingInPart := partFile.FileSize - localOffset
		
		bytesToReadFromThisPart := bytesRemainingInRequest
		if bytesRemainingInPart < bytesRemainingInRequest {
			bytesToReadFromThisPart = bytesRemainingInPart
		}

		if bytesToReadFromThisPart <= 0 {
			// This shouldn't happen, but as a safeguard
			partIndex++
			localOffset = 0
			continue
		}

		partStart := localOffset
		partEnd := localOffset + bytesToReadFromThisPart - 1

		// 4. Create a *single file* reader for this part's range
		partReader, err := NewTelegramReader(r.ctx, r.client, partFile.Location, partStart, partEnd, bytesToReadFromThisPart)
		if err != nil {
			r.pipeWriter.CloseWithError(fmt.Errorf("failed to create part reader: %w", err))
			return
		}

		// 5. Copy this part's data to the pipe
		n, err := io.Copy(r.pipeWriter, partReader)
		partReader.Close() // Close the single-part reader
		
		if err != nil {
			r.pipeWriter.CloseWithError(fmt.Errorf("failed to stream part %d: %w", partMessageID, err))
			return
		}

		bytesWritten += n

		// 6. Prepare for the next part
		partIndex++
		localOffset = 0 // Subsequent parts are always read from their beginning
	}
}