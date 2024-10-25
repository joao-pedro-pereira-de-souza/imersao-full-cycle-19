package converter

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"time"
)

type VideoConverter struct {
	db *sql.DB
}

func NewVideoConverter(db *sql.DB) *VideoConverter {
	return &VideoConverter{
		db: db,
	}
}

type VideoTask struct {
	VideoId int    `json:"video_id"`
	Path    string `json:"path"`
}

func (vc *VideoConverter) Hendle(msg []byte) {
	var task VideoTask

	err := json.Unmarshal(msg, &task)
	if err != nil {
		vc.logError(task, "failed to unmarshaltask", err)

		return
	}

	if isProcessed(vc.db, task.VideoId) {
		slog.Warn("Video already processed", slog.Int("video_id", task.VideoId))

		return
	}

	err = vc.processVideo(&task)
	if err != nil {
		vc.logError(task, "failed to process video", err)

		return
	}

	err = MarkProcessed(vc.db, task.VideoId)

	if err != nil {
		vc.logError(task, "failed to mark video as processed", err)

		return
	}

	slog.Info("Video marked as processed", slog.Int("video_id", task.VideoId))

}

func (vc *VideoConverter) processVideo(task *VideoTask) error {
	mergedFile := filepath.Join(task.Path, "merged.mp4")
	mpegDashPath := filepath.Join(task.Path, "mpeg-dash")

	slog.Info("Merging chunks", slog.String("path", task.Path))

	err := vc.mergeChunks(task.Path, mergedFile)
	if err != nil {
		vc.logError(*task, "failed to merge chunks", err)
		return err
	}

	slog.Info("create mpeg-dash", slog.String("path", mpegDashPath))

	err = os.MkdirAll(mpegDashPath, os.ModePerm)
	if err != nil {
		vc.logError(*task, "failed to MkdirAll", err)
		return err
	}

	slog.Info("Converter video to mpeg-dash", slog.String("path", mpegDashPath))
	ffpegCmd := exec.Command(
		"ffmpeg", "-i", mergedFile,
		"-f", "dash",
		filepath.Join(mpegDashPath, "output.mpd"),
	)

	output, err := ffpegCmd.CombinedOutput()
	if err != nil {
		vc.logError(*task, "failed to convert video to mpeg-dash, output"+string(output), err)
		return err
	}

	slog.Info("Video converted to mpeg-dash", slog.String("path", mpegDashPath))

	err = os.Remove(mergedFile)
	if err != nil {
		vc.logError(*task, "failed remove file merged", err)
		return err
	}

	return nil
}

func (vc *VideoConverter) logError(task VideoTask, message string, err error) {
	errorData := map[string]any{
		"video_id": task.VideoId,
		"error":    message,
		"details":  err.Error(),
		"time":     time.Now(),
	}

	serializedError, _ := json.Marshal(errorData)
	slog.Error("processing error", slog.String("error_details", string(serializedError)))

	RegisterError(vc.db, errorData, err)
}
func (vc *VideoConverter) extractNumber(fileName string) int {
	re := regexp.MustCompile(`\d+`)

	numStr := re.FindString(filepath.Base(fileName))
	num, err := strconv.Atoi(numStr)

	if err != nil {
		return -1
	}

	return num
}

func (vc *VideoConverter) mergeChunks(inputDir, outputFile string) error {
	chunks, err := filepath.Glob(filepath.Join(inputDir, "*.chunk"))

	if err != nil {
		return fmt.Errorf("failed to find chunks: %v", err)
	}

	sort.Slice(chunks, func(i, j int) bool {
		return vc.extractNumber(chunks[i]) < vc.extractNumber(chunks[j])
	})

	output, err := os.Create(outputFile)

	if err != nil {
		return fmt.Errorf("failed to create outpot file %v", err)
	}

	defer output.Close()

	for _, chunk := range chunks {
		input, err := os.Open(chunk)

		if err != nil {
			return fmt.Errorf("failed to open chunk %v", err)
		}

		_, err = output.ReadFrom((input))
		if err != nil {
			return fmt.Errorf("failed to write chunk %s to merged file: %v", chunk, err)
		}

		input.Close()
	}

	return nil
}
