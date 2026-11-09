// Package filesystem provides file and directory helpers for the uploaded app sources and the build directories.
package filesystem

import (
	"fmt"
	"io"
	"mime/multipart"
	"os"
	"path/filepath"
)

// SaveMultiPartFile saves an uploaded file into saveLocation under its original file name.
func SaveMultiPartFile(fileHeader *multipart.FileHeader, saveLocation string) error {
	file, err := fileHeader.Open()
	if err != nil {
		return err
	}
	// Opened for reading only, so a failed close cannot lose data.
	defer func() { _ = file.Close() }()
	savePath := filepath.Join(saveLocation, fileHeader.Filename)
	outFile, err := os.Create(savePath)
	if err != nil {
		return err
	}
	// Best-effort close: this function does not report close errors to its caller.
	defer func() { _ = outFile.Close() }()

	_, err = io.Copy(outFile, file)
	if err != nil {
		return err
	}

	return nil
}

// CopyFileToDstFolder copies a file into dstFolder under the same name and returns the path of the copy.
func CopyFileToDstFolder(source string, dstFolder string) (string, error) {
	srcFile, err := os.Open(source)
	if err != nil {
		return "", fmt.Errorf("failed to open source file: %v", err)
	}
	// Opened for reading only, so a failed close cannot lose data.
	defer func() { _ = srcFile.Close() }()

	_, fileName := filepath.Split(source)
	destPath := filepath.Join(dstFolder, fileName)

	dstFile, err := os.Create(destPath)
	if err != nil {
		return "", fmt.Errorf("failed to create destination file: %v", err)
	}
	// Best-effort close: this function does not report close errors to its caller.
	defer func() { _ = dstFile.Close() }()

	_, err = io.Copy(dstFile, srcFile)
	if err != nil {
		return "", fmt.Errorf("failed to copy file content: %v", err)
	}

	return destPath, nil
}

// CreateDir creates a directory with all missing parents; an existing directory is not an error.
func CreateDir(path string) error {
	return os.MkdirAll(path, os.ModePerm)
}

// DeleteDir removes a directory with all its contents.
func DeleteDir(path string) error {
	err := os.RemoveAll(path)
	if err != nil {
		return fmt.Errorf("failed to delete folder %s: %w", path, err)
	}
	return nil
}

// FileExists reports whether a file exists at path. A stat error other than "not exist" also counts as existing.
func FileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil || !os.IsNotExist(err)
}

// CopyFilesByNames copies the files of srcDir whose names are in filenames into destDir and returns the paths of
// the copies. Subdirectories are searched only if recursive is set.
func CopyFilesByNames(srcDir, destDir string, filenames []string, recursive bool) ([]string, error) {
	var copiedFiles []string

	filenameMap := make(map[string]bool, len(filenames))

	for _, name := range filenames {
		filenameMap[name] = true
	}

	err := filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !recursive && info.IsDir() && path != srcDir {
			return filepath.SkipDir
		}

		if !info.IsDir() {
			if _, ok := filenameMap[info.Name()]; ok {
				resPath, err := CopyFileToDstFolder(path, destDir)
				if err != nil {
					return err
				}
				copiedFiles = append(copiedFiles, resPath)
			}
		}

		return nil
	})

	return copiedFiles, err
}
