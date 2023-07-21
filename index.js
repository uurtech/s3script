const AWS = require('aws-sdk');
const s3 = new AWS.S3();

const bucketName = 'ugur-test';
const prefix = 'log_date=2023-06-01/';

function getObjects(bucket, prefix) {
  return new Promise((resolve, reject) => {
    let objects = [];

    const listObjectsParams = {
      Bucket: bucket,
      Prefix: prefix,
    };

    function listObjects() {
      s3.listObjectsV2(listObjectsParams, (err, data) => {
        if (err) {
          return reject(err);
        }

        if (data.Contents) {
          objects = objects.concat(data.Contents);
        }

        if (data.NextContinuationToken) {
          listObjectsParams.ContinuationToken = data.NextContinuationToken;
          listObjects();
        } else {
          resolve(objects);
        }
      });
    }

    listObjects();
  });
}

async function searchFilesInS3Bucket(bucketName, prefix = '') {
  const filesBySize = {};

  async function processPage(params) {
    try {
      const data = await s3.listObjectsV2(params).promise();

      if (data.Contents) {
        for (const obj of data.Contents) {
          const key = obj.Key;
          if (!key.endsWith('/')) {
            const size = obj.Size;
            if (size in filesBySize) {
              filesBySize[size].push(key);
            } else {
              filesBySize[size] = [key];
            }
          }
        }
      }

      if (data.NextContinuationToken) {
        params.ContinuationToken = data.NextContinuationToken;
        await processPage(params);
      }
    } catch (err) {
      console.error('Error processing page:', err);
    }
  }

  try {
    await processPage({ Bucket: bucketName, Prefix: prefix });

    const filesList = Object.entries(filesBySize)
      .filter(([, files]) => files.length > 1)
      .map(([size, files]) => ({ size, files }));

    const folderList = filesList[0].files.map(file => {
      const splitted = file.split('/');
      splitted.pop();
      return splitted;
    });

    console.log(folderList);
  } catch (err) {
    console.error('Error searching files in S3 bucket:', err);
  }
}

async function getLastModifiedFiles(bucketName, filePaths) {
  const lastModifiedFiles = [];

  for (const filePath of filePaths) {
    try {
      const headObjectParams = {
        Bucket: bucketName,
        Key: filePath,
      };

      const data = await s3.headObject(headObjectParams).promise();
      const lastModified = data.LastModified;
      lastModifiedFiles.push({ filePath, lastModified });
    } catch (err) {
      console.error(`Error retrieving metadata for file ${filePath}: ${err}`);
    }
  }

  lastModifiedFiles.sort((a, b) => b.lastModified - a.lastModified);
  return lastModifiedFiles;
}

exports.handler = async (event, context) => {
  try {
    await searchFilesInS3Bucket(bucketName, prefix);
    return {
      message: 'muhittin',
    };
  } catch (err) {
    console.error('Error in Lambda handler:', err);
    return {
      error: err.message || 'Unknown error occurred.',
    };
  }
};
