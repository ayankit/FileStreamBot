package routes

import (
	"EverythingSuckz/fsb/config"
	"EverythingSuckz/fsb/internal/bot"
	"EverythingSuckz/fsb/internal/types"
	"EverythingSuckz/fsb/internal/utils"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/gotd/td/tg"
	range_parser "github.com/quantumsheep/range-parser"
	"go.uber.org/zap"

	"github.com/gin-gonic/gin"
)

var log *zap.Logger

func (e *allRoutes) LoadHome(r *Route) {
	log = e.log.Named("Stream")
	defer log.Info("Loaded stream route")
	r.Engine.GET("/stream/:messageID", getStreamRoute)
}

func getStreamRoute(ctx *gin.Context) {
	w := ctx.Writer
	r := ctx.Request

	messageIDParm := ctx.Param("messageID")
	messageID, err := strconv.Atoi(messageIDParm)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	apiKey := ctx.Query("apiKey")
	if apiKey == "" {
		http.Error(w, "missing apiKey param", http.StatusBadRequest)
		return
	}

	if !utils.Contains(config.ValueOf.ApiKeys, apiKey) {
		http.Error(w, "invalid apiKey", http.StatusForbidden)
		return
	}

	worker := bot.GetNextWorker()

	tgMessage, err := utils.GetTGMessage(ctx, worker.Client, messageID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if tgMessage.Media != nil {
		handleSingleFileStream(ctx, worker, messageID, r, w)
		return
	}

	if tgMessage.Message != "" {
		manifest, err := utils.ParseManifest(tgMessage.Message)
		if err != nil {
			http.Error(w, "message is not a valid manifest: "+err.Error(), http.StatusBadRequest)
			return
		}

		handleMultiPartStream(ctx, worker, manifest, r, w)
		return
	}

	http.Error(w, "Unsupported message type", http.StatusBadRequest)
}

func handleSingleFileStream(ctx *gin.Context, worker *bot.Worker, messageID int, r *http.Request, w http.ResponseWriter) {
	file, err := utils.FileFromMessage(ctx, worker.Client, messageID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// for photo messages
	if file.FileSize == 0 {
		res, err := worker.Client.API().UploadGetFile(ctx, &tg.UploadGetFileRequest{
			Location: file.Location,
			Offset:   0,
			Limit:    1024 * 1024,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		result, ok := res.(*tg.UploadFile)
		if !ok {
			http.Error(w, "unexpected response", http.StatusInternalServerError)
			return
		}
		fileBytes := result.GetBytes()
		ctx.Header("Content-Disposition", fmt.Sprintf("inline; filename=\"%s\"", file.FileName))
		if r.Method != "HEAD" {
			ctx.Data(http.StatusOK, file.MimeType, fileBytes)
		}
		return
	}

	ctx.Header("Accept-Ranges", "bytes")
	var start, end int64
	rangeHeader := r.Header.Get("Range")

	if rangeHeader == "" {
		start = 0
		end = file.FileSize - 1
		w.WriteHeader(http.StatusOK)
	} else {
		ranges, err := range_parser.Parse(file.FileSize, r.Header.Get("Range"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		start = ranges[0].Start
		end = ranges[0].End
		ctx.Header("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, file.FileSize))
		log.Info("Content-Range", zap.Int64("start", start), zap.Int64("end", end), zap.Int64("fileSize", file.FileSize))
		w.WriteHeader(http.StatusPartialContent)
	}

	contentLength := end - start + 1
	mimeType := file.MimeType

	if mimeType == "" {
		mimeType = "application/octet-stream"
	}

	ctx.Header("Content-Type", mimeType)
	ctx.Header("Content-Length", strconv.FormatInt(contentLength, 10))

	disposition := "inline"

	if ctx.Query("d") == "true" {
		disposition = "attachment"
	}

	ctx.Header("Content-Disposition", fmt.Sprintf("%s; filename=\"%s\"", disposition, file.FileName))

	if r.Method != "HEAD" {
		lr, _ := utils.NewTelegramReader(ctx, worker.Client, file.Location, start, end, contentLength)
		if _, err := io.CopyN(w, lr, contentLength); err != nil {
			log.Error("Error while copying stream", zap.Error(err))
		}
	}
}

func handleMultiPartStream(ctx *gin.Context, worker *bot.Worker, manifest *types.Manifest, r *http.Request, w http.ResponseWriter) {
	ctx.Header("Accept-Ranges", "bytes")
	var start, end int64
	rangeHeader := r.Header.Get("Range")
	
	totalSize := manifest.TotalSize

	if rangeHeader == "" {
		start = 0
		end = totalSize - 1
		w.WriteHeader(http.StatusOK)
	} else {
		ranges, err := range_parser.Parse(totalSize, r.Header.Get("Range"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		start = ranges[0].Start
		end = ranges[0].End
		ctx.Header("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, totalSize))
		log.Info("Content-Range (multi-part)", zap.Int64("start", start), zap.Int64("end", end), zap.Int64("fileSize", totalSize))
		w.WriteHeader(http.StatusPartialContent)
	}

	contentLength := end - start + 1
	mimeType := manifest.MimeType

	ctx.Header("Content-Type", mimeType)
	ctx.Header("Content-Length", strconv.FormatInt(contentLength, 10))

	disposition := "inline"
	if ctx.Query("d") == "true" {
		disposition = "attachment"
	}

	ctx.Header("Content-Disposition", fmt.Sprintf("%s; filename=\"%s\"", disposition, manifest.FileName))

	if r.Method != "HEAD" {
		lr, err := utils.NewMultiPartTelegramReader(ctx, worker.Client, manifest, start, end, contentLength)
		if err != nil {
			http.Error(w, "Failed to initialize multi-part reader: "+err.Error(), http.StatusInternalServerError)
			return
		}
		
		if _, err := io.CopyN(w, lr, contentLength); err != nil {
			log.Error("Error while copying multi-part stream", zap.Error(err))
		}
		lr.Close()
	}
}