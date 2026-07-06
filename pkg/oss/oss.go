package oss

type Client interface {
	Upload(filePath string) (*UploadRes, error)
	UploadWithCustomPath(filePath, customDir, customFilename, fileType string) (*UploadRes, error)
	UploadWithOptions(filePath string, opts UploadOptions) (*UploadRes, error)
	DownloadToFile(localPath string, opts DownloadOptions) (*DownloadRes, error)
}

type UploadRes struct {
	URL         string
	ProviderURL string
	FileSize    int64
	OSSKey      string
	AssetID     int64
}

type UploadOptions struct {
	CustomDir      string
	CustomFilename string
	FileType       string

	AssetClass   string
	AssetKind    string
	Visibility   string
	Source       string
	BusinessType string
	TaskID       int64
	NodeName     string
	OwnerType    string
	OwnerID      int64
	Protect      bool
}

type DownloadOptions struct {
	ObjectKey string
	AssetID   int64
	OwnerID   int64
}

type DownloadRes struct {
	LocalPath string
	OSSKey    string
	AssetID   int64
	FileSize  int64
}
