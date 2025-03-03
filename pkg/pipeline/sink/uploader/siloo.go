package uploader

import (
	"bytes"
	"encoding/json"
	"github.com/livekit/egress/pkg/errors"
	"github.com/livekit/egress/pkg/types"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

type GetFileCredsReq struct {
	FileSize int64  `json:"file_size"`
	FilePath string `json:"file_path"`
	FileType string `json:"file_type"`
}

type GetFileCredsResp struct {
	UploadURL      string `json:"upload_url"`
	ReturnLocation string `json:"return_location"`
}

type SilooUploader struct {
	grahamAddress string
}

func NewSilooUploader(grahamAddress string) (*SilooUploader, error) {
	silooUploader := &SilooUploader{
		grahamAddress: grahamAddress,
	}

	return silooUploader, nil
}

func (s *SilooUploader) upload(localFilepath, storageFilepath string, outputType types.OutputType) (string, int64, error) {
	fileStats, err := os.Stat(localFilepath)
	if err != nil {
		return "", 0, errors.ErrUploadFailed(storageFilepath, errors.New("Failed to get file stats: "+err.Error()))
	}

	if fileStats.Size() == 0 {
		return "", 0, errors.ErrUploadFailed(storageFilepath, errors.New("File is empty"))
	}

	req := &GetFileCredsReq{
		FileSize: fileStats.Size(),
		FilePath: storageFilepath,
		FileType: string(outputType),
	}
	reqBytes, err := json.Marshal(req)
	if err != nil {
		return "", 0, errors.ErrUploadFailed(storageFilepath, errors.New("Failed to marshal GetFileCredsReq"+err.Error()))
	}

	var resp *http.Response
	for range 60 {
		resp, err = http.Post(s.grahamAddress, "text/json", bytes.NewReader(reqBytes))
		if err == nil && resp.StatusCode == 200 {
			break
		} else {
			if err != nil {
				log.Println("Failed to get url, retrying?: " + err.Error())
			} else {
				log.Println("Failed to get url, retrying?: status code: " + resp.Status)
				err = errors.New("Failed to get url: " + resp.Status)
			}
		}
		time.Sleep(time.Minute)
	}
	if err != nil {
		return "", 0, errors.ErrUploadFailed(storageFilepath, errors.New("Failed to send req to Graham: "+err.Error()))
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", 0, errors.ErrUploadFailed(storageFilepath, errors.New("Failed to read resp body: "+err.Error()))
	}

	respObj := new(GetFileCredsResp)
	err = json.Unmarshal(respBytes, respObj)
	if err != nil {
		return "", 0, errors.ErrUploadFailed(storageFilepath, errors.New("Failed to unmarshal response: "+err.Error()+". response is: "+string(respBytes)))
	}

	var fileResp *http.Response
	var file *os.File
	var fileReq *http.Request
	for range 60 {
		if file != nil {
			file.Close()
		}
		file, err = os.Open(localFilepath)
		if err != nil {
			return "", 0, errors.ErrUploadFailed(storageFilepath, errors.New("Failed to open file: "+err.Error()))
		}

		fileReq, err = http.NewRequest("PUT", respObj.UploadURL, file)
		if err != nil {
			file.Close()
			return "", 0, errors.ErrUploadFailed(storageFilepath, errors.New("Failed to create request: "+err.Error()))
		}

		fileResp, err = http.DefaultClient.Do(fileReq)
		if err == nil && fileResp.StatusCode == http.StatusOK {
			defer fileResp.Body.Close()
			break
		} else {
			var errorText string
			if err == nil {
				errorText = "Non 200 status code " + fileResp.Status
			} else {
				errorText = err.Error()
			}
			log.Println("Failed to upload, retrying?: " + errorText)
		}
		time.Sleep(time.Minute)
	}
	if err != nil {
		return "", 0, errors.ErrUploadFailed(storageFilepath, errors.New("Failed to upload to siloo: "+err.Error()))
	}
	if fileResp.StatusCode != http.StatusOK {
		return "", 0, errors.ErrUploadFailed(storageFilepath, errors.New("File upload resp not OK: "+resp.Status))
	}
	if file != nil {
		file.Close()
	}

	return respObj.ReturnLocation, fileStats.Size(), nil
}
