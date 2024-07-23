package uploader

import (
	"bytes"
	"encoding/json"
	"github.com/livekit/egress/pkg/errors"
	"github.com/livekit/egress/pkg/types"
	"io"
	"net/http"
	"os"
)

type getFileCredsReq struct {
	FileSize int64  `json:"file_size"`
	FilePath string `json:"file_path"`
	FileType string `json:"file_type"`
}

type getFileCredsResp struct {
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
		return "", 0, errors.New("Failed to get file stats: " + err.Error())
	}

	req := &getFileCredsReq{
		FileSize: fileStats.Size(),
		FilePath: storageFilepath,
		FileType: string(outputType),
	}
	reqBytes, err := json.Marshal(req)
	if err != nil {
		return "", 0, errors.New("Failed to marshal req: " + err.Error())
	}

	resp, err := http.Post(s.grahamAddress, "text/json", bytes.NewReader(reqBytes))
	if err != nil {
		return "", 0, errors.New("Failed to send req to Graham: " + err.Error())
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", 0, errors.New("Failed to read resp body: " + err.Error())
	}

	respObj := new(getFileCredsResp)
	err = json.Unmarshal(respBytes, respObj)
	if err != nil {
		return "", 0, errors.New("Failed to unmarshal response: " + err.Error())
	}

	file, err := os.Open(localFilepath)
	if err != nil {
		return "", 0, errors.New("Failed ot open file: " + err.Error())
	}
	defer file.Close()

	fileReq, err := http.NewRequest("PUT", respObj.UploadURL, file)
	if err != nil {
		return "", 0, errors.New("Failed to create upload req: " + err.Error())
	}
	fileResp, err := http.DefaultClient.Do(fileReq)
	if err != nil {
		return "", 0, errors.New("Failed to do file upload req: " + err.Error())
	}
	if resp.StatusCode != http.StatusOK {
		return "", 0, errors.New("File upload resp not OK: " + resp.Status)
	}
	_ = fileResp.Body.Close()

	return respObj.ReturnLocation, fileStats.Size(), nil
}
