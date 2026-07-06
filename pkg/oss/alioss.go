package oss

import (
	"fmt"
	"github.com/tuxi/flux-workflow/domain/entity"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gorm.io/gorm"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

type AliOSSClient struct {
	// OSS核心配置（建议从环境变量/配置文件读取，而非硬编码）
	ossEndpoint        string // OSS地域节点，如：oss-cn-hangzhou.aliyuncs.com
	ossAccessKeyID     string // AccessKey ID
	ossAccessKeySecret string // AccessKey Secret
	ossBucketName      string // Bucket名称
	ossBucketRegion    string // Bucket的访问域名，如：your-bucket.oss-cn-hangzhou.aliyuncs.com
	db                 *gorm.DB
	accessMode         string
	providerURLTTL     int64
}

type AliOSSClientOptions struct {
	AccessMode               string
	ProviderURLExpireSeconds int64
}

// NewAliOSS 初始化OSS上传工具
func NewAliOSSClient(endpoint, accessKeyID, accessKeySecret, bucketName, ossBucketRegion string) Client {
	return NewAliOSSClientWithDB(endpoint, accessKeyID, accessKeySecret, bucketName, ossBucketRegion, nil)
}

func NewAliOSSClientWithDB(endpoint, accessKeyID, accessKeySecret, bucketName, ossBucketRegion string, db *gorm.DB) Client {
	return NewAliOSSClientWithDBOptions(endpoint, accessKeyID, accessKeySecret, bucketName, ossBucketRegion, db, AliOSSClientOptions{})
}

func NewAliOSSClientWithDBOptions(endpoint, accessKeyID, accessKeySecret, bucketName, ossBucketRegion string, db *gorm.DB, opts AliOSSClientOptions) Client {
	accessMode := strings.ToLower(strings.TrimSpace(opts.AccessMode))
	if accessMode == "" {
		accessMode = "public"
	}
	providerURLTTL := opts.ProviderURLExpireSeconds
	if providerURLTTL <= 0 {
		providerURLTTL = 2 * 60 * 60
	}
	return &AliOSSClient{
		ossEndpoint:        endpoint,
		ossAccessKeyID:     accessKeyID,
		ossAccessKeySecret: accessKeySecret,
		ossBucketName:      bucketName,
		ossBucketRegion:    ossBucketRegion,
		db:                 db,
		accessMode:         accessMode,
		providerURLTTL:     providerURLTTL,
	}
}

// UploadWithCustomPath 带自定义路径的上传方法
func (t *AliOSSClient) UploadWithCustomPath(filePath, customDir, fileNamePrex, fileType string) (*UploadRes, error) {
	return t.UploadWithOptions(filePath, UploadOptions{
		CustomDir:      customDir,
		CustomFilename: fileNamePrex,
		FileType:       fileType,
	})
}

func (t *AliOSSClient) UploadWithOptions(filePath string, opts UploadOptions) (*UploadRes, error) {
	localFilePath := filePath

	// 1. 校验本地文件是否存在
	fileInfo, err := os.Stat(localFilePath)
	if err != nil {
		log.Printf("文件校验失败：%s, err: %v", localFilePath, err)
		return nil, fmt.Errorf("本地文件不存在或无访问权限：%s", localFilePath)
	}
	if fileInfo.IsDir() {
		log.Printf("路径不是文件：%s", localFilePath)
		return nil, fmt.Errorf("路径不是文件：%s", localFilePath)
	}

	// 2. 初始化OSS客户端
	client, err := oss.New(t.ossEndpoint, t.ossAccessKeyID, t.ossAccessKeySecret)
	if err != nil {
		log.Printf("OSS客户端初始化失败：%v", err)
		return nil, fmt.Errorf("阿里云OSS客户端初始化失败")
	}

	// 3. 获取Bucket实例
	bucket, err := client.Bucket(t.ossBucketName)
	if err != nil {
		log.Printf("获取Bucket失败：%s, err: %v", t.ossBucketName, err)
		return nil, fmt.Errorf("获取OSS Bucket失败：%s", t.ossBucketName)
	}

	// 4. 构造OSS存储Key（优先使用自定义路径，兜底用原有逻辑）
	fileExt := filepath.Ext(localFilePath)           // 获取文件后缀（如.mp4）
	originalFileName := filepath.Base(localFilePath) // 原文件名
	timeDir := time.Now().Format("20060102")         // 时间目录（兜底用）
	timestamp := time.Now().UnixNano()               // 时间戳（防重名）

	// 4.1 确定存储目录
	var ossDir string
	if opts.CustomDir != "" {
		// 清理自定义目录的非法字符，确保路径规范
		ossDir = strings.Trim(opts.CustomDir, "/")
	} else if opts.FileType != "" {
		// 未传自定义目录，但传了file_type → 按类型+时间目录
		ossDir = fmt.Sprintf("%s/%s", opts.FileType, timeDir)
	} else {
		// 兜底：仅时间目录
		ossDir = timeDir
	}

	// 4.2 确定文件名
	var ossFilename string
	if opts.CustomFilename != "" {
		// 自定义文件名 + 原后缀
		ossFilename = fmt.Sprintf("%s_%d%s", opts.CustomFilename, timestamp, fileExt)
	} else {
		// 兜底：时间戳 + 原文件名（防重名）
		ossFilename = fmt.Sprintf("%d_%s", timestamp, originalFileName)
	}

	// 4.3 拼接最终OSS Key
	ossKey := fmt.Sprintf("%s/%s", ossDir, ossFilename)
	// 移除多余的斜杠（如自定义目录以/结尾时）
	ossKey = strings.ReplaceAll(ossKey, "//", "/")

	// 5. 上传文件到OSS
	log.Println("开始上传文件：", map[string]any{
		"local_path": localFilePath,
		"oss_key":    ossKey,
		"file_size":  fileInfo.Size(),
	})
	err = bucket.PutObjectFromFile(ossKey, localFilePath)
	if err != nil {
		errMsg := fmt.Sprintf("文件上传到OSS失败：%s -> %s", localFilePath, ossKey)
		log.Printf("%s, err: %v", errMsg, err)
		return nil, fmt.Errorf(errMsg+": %w", err)
	}

	// 6. 生成带签名的可访问URL（可选：替换为之前的签名URL逻辑）
	ossURL := fmt.Sprintf("https://%s/%s", t.ossBucketRegion, ossKey)

	// 7. 上传成功日志
	log.Println("上传文件成功：", map[string]any{
		"local_path": localFilePath,
		"oss_key":    ossKey,
		"oss_url":    ossURL,
		"file_size":  fileInfo.Size(),
	})

	assetID := int64(0)
	if t.db != nil && opts.AssetClass != "" {
		assetID = t.recordStorageObject(ossKey, ossURL, fileInfo.Size(), fileExt, opts)
	}

	// 8. 返回结果
	return &UploadRes{
		URL:         ossURL,
		ProviderURL: t.providerURL(ossURL, ossKey, opts),
		OSSKey:      ossKey,
		FileSize:    fileInfo.Size(),
		AssetID:     assetID,
	}, nil
}

func (t *AliOSSClient) providerURL(ossURL, ossKey string, opts UploadOptions) string {
	if opts.Visibility != "provider_access" {
		return ossURL
	}
	if t.accessMode != "private" {
		return ossURL
	}
	signedURL, err := t.signGetURL(ossKey, t.providerURLTTL)
	if err != nil {
		log.Printf("生成 provider_access 短签 URL 失败：oss_key=%s err=%v", ossKey, err)
		return ossURL
	}
	return signedURL
}

// 兼容原有Upload方法（调用新方法，不传自定义参数）
func (t *AliOSSClient) Upload(filePath string) (*UploadRes, error) {
	return t.UploadWithCustomPath(filePath, "", "", "")
}

func (t *AliOSSClient) DownloadToFile(localPath string, opts DownloadOptions) (*DownloadRes, error) {
	objectKey := strings.TrimSpace(opts.ObjectKey)
	assetID := opts.AssetID
	if objectKey == "" && assetID > 0 {
		if t.db == nil {
			return nil, fmt.Errorf("asset download requires db-backed oss client")
		}
		var obj entity.StorageObject
		if err := t.db.First(&obj, "id = ?", assetID).Error; err != nil {
			return nil, err
		}
		if obj.OwnerType == "user" && opts.OwnerID > 0 && obj.OwnerID != opts.OwnerID {
			return nil, fmt.Errorf("asset does not belong to current user")
		}
		objectKey = obj.OSSKey
	}
	if objectKey == "" {
		return nil, fmt.Errorf("object_key or asset_id is required")
	}

	client, err := oss.New(t.ossEndpoint, t.ossAccessKeyID, t.ossAccessKeySecret)
	if err != nil {
		return nil, fmt.Errorf("阿里云OSS客户端初始化失败")
	}
	bucket, err := client.Bucket(t.ossBucketName)
	if err != nil {
		return nil, fmt.Errorf("获取OSS Bucket失败：%s", t.ossBucketName)
	}
	if err := bucket.GetObjectToFile(objectKey, localPath); err != nil {
		return nil, err
	}
	info, _ := os.Stat(localPath)
	size := int64(0)
	if info != nil {
		size = info.Size()
	}
	return &DownloadRes{
		LocalPath: localPath,
		OSSKey:    objectKey,
		AssetID:   assetID,
		FileSize:  size,
	}, nil
}

func (t *AliOSSClient) signGetURL(ossKey string, expireSeconds int64) (string, error) {
	if expireSeconds <= 0 {
		expireSeconds = 2 * 60 * 60
	}
	client, err := oss.New(t.ossEndpoint, t.ossAccessKeyID, t.ossAccessKeySecret)
	if err != nil {
		return "", err
	}
	bucket, err := client.Bucket(t.ossBucketName)
	if err != nil {
		return "", err
	}
	return bucket.SignURL(ossKey, oss.HTTPGet, expireSeconds)
}

func (t *AliOSSClient) recordStorageObject(ossKey, ossURL string, fileSize int64, fileExt string, opts UploadOptions) int64 {
	ownerType := strings.TrimSpace(opts.OwnerType)
	if ownerType == "" {
		ownerType = "user"
	}
	visibility := strings.TrimSpace(opts.Visibility)
	if visibility == "" {
		visibility = defaultVisibility(opts.AssetClass)
	}
	source := strings.TrimSpace(opts.Source)
	if source == "" {
		source = "server_upload"
	}
	assetKind := strings.TrimSpace(opts.AssetKind)
	if assetKind == "" {
		assetKind = kindFromExt(fileExt)
	}

	var taskID *int64
	if opts.TaskID > 0 {
		taskID = &opts.TaskID
	}

	obj := &entity.StorageObject{
		Bucket:           t.ossBucketName,
		OSSKey:           ossKey,
		URL:              ossURL,
		OwnerType:        ownerType,
		OwnerID:          opts.OwnerID,
		AssetClass:       opts.AssetClass,
		AssetKind:        assetKind,
		Visibility:       visibility,
		Source:           source,
		BusinessType:     strings.TrimSpace(opts.BusinessType),
		TaskID:           taskID,
		NodeName:         strings.TrimSpace(opts.NodeName),
		Status:           entity.StorageObjectStatusActive,
		Protect:          opts.Protect,
		SizeBytes:        fileSize,
		ContentType:      contentTypeFromExt(fileExt),
		OriginalFilename: filepath.Base(ossKey),
	}
	if err := t.db.Create(obj).Error; err != nil {
		log.Printf("记录OSS资产失败：oss_key=%s err=%v", ossKey, err)
		return 0
	}
	return obj.ID
}

func defaultVisibility(assetClass string) string {
	switch assetClass {
	case "admin_catalog":
		return "public"
	case "task_intermediate", "cache", "audit":
		return "internal"
	default:
		return "private"
	}
}

func kindFromExt(ext string) string {
	switch strings.ToLower(ext) {
	case ".jpg", ".jpeg", ".png", ".webp", ".gif", ".heic":
		return "image"
	case ".mp4", ".mov", ".m4v", ".webm":
		return "video"
	case ".mp3", ".wav", ".m4a", ".aac":
		return "audio"
	case ".json":
		return "json"
	default:
		return "file"
	}
}

func contentTypeFromExt(ext string) string {
	switch strings.ToLower(ext) {
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".png":
		return "image/png"
	case ".webp":
		return "image/webp"
	case ".gif":
		return "image/gif"
	case ".mp4":
		return "video/mp4"
	case ".mov":
		return "video/quicktime"
	case ".mp3":
		return "audio/mpeg"
	case ".wav":
		return "audio/wav"
	case ".json":
		return "application/json"
	default:
		return ""
	}
}
