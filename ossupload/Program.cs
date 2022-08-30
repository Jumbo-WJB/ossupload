/*
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace Aliyun.OSS.Samples
{
    public class MultipartUploadSample
    {


        static int partSize = 50 * 1024 * 1024;

        public class UploadPartContext
        {
            public string BucketName { get; set; }
            public string ObjectName { get; set; }

            public List<PartETag> PartETags { get; set; }

            public string UploadId { get; set; }
            public long TotalParts { get; set; }
            public long CompletedParts { get; set; }
            public object SyncLock { get; set; }
            public ManualResetEvent WaitEvent { get; set; }
        }

        public class UploadPartContextWrapper
        {
            public UploadPartContext Context { get; set; }
            public int PartNumber { get; set; }
            public Stream PartStream { get; set; }

            public UploadPartContextWrapper(UploadPartContext context, Stream partStream, int partNumber)
            {
                Context = context;
                PartStream = partStream;
                PartNumber = partNumber;
            }
        }

        public class UploadPartCopyContext
        {
            public string TargetBucket { get; set; }
            public string TargetObject { get; set; }

            public List<PartETag> PartETags { get; set; }

            public string UploadId { get; set; }
            public long TotalParts { get; set; }
            public long CompletedParts { get; set; }
            public object SyncLock { get; set; }
            public ManualResetEvent WaitEvent { get; set; }
        }

        public class UploadPartCopyContextWrapper
        {
            public UploadPartCopyContext Context { get; set; }
            public int PartNumber { get; set; }

            public UploadPartCopyContextWrapper(UploadPartCopyContext context, int partNumber)
            {
                Context = context;
                PartNumber = partNumber;
            }
        }

        /// <summary>
        /// 分片上传。
        /// </summary>
        public static void UploadMultipart(String bucketName,String fileToUpload,String key, OssClient client)
        {
            var uploadId = InitiateMultipartUpload(bucketName, key, client);
            var partETags = UploadParts(bucketName, key, fileToUpload, uploadId, partSize, client);
            CompleteUploadPart(bucketName, key, uploadId, partETags,client);

            Console.WriteLine("Multipart put object:{0} succeeded", key);
        }


        private static string InitiateMultipartUpload(String bucketName, String objectName, OssClient client)
        {
            var request = new InitiateMultipartUploadRequest(bucketName, objectName);
            var result = client.InitiateMultipartUpload(request);
            return result.UploadId;
        }

        private static List<PartETag> UploadParts(String bucketName, String objectName, String fileToUpload,
                                                  String uploadId, int partSize,OssClient client)
        {
            var fi = new FileInfo(fileToUpload);
            var fileSize = fi.Length;
            var partCount = fileSize / partSize;
            if (fileSize % partSize != 0)
            {
                partCount++;
            }

            var partETags = new List<PartETag>();
            using (var fs = File.Open(fileToUpload, FileMode.Open))
            {
                for (var i = 0; i < partCount; i++)
                {
                    var skipBytes = (long)partSize * i;
                    fs.Seek(skipBytes, 0);
                    var size = (partSize < fileSize - skipBytes) ? partSize : (fileSize - skipBytes);
                    var request = new UploadPartRequest(bucketName, objectName, uploadId)
                    {
                        InputStream = fs,
                        PartSize = size,
                        PartNumber = i + 1
                    };

                    var result = client.UploadPart(request);

                    partETags.Add(result.PartETag);
                    Console.WriteLine("finish {0}/{1}", partETags.Count, partCount);
                }
            }
            return partETags;
        }
        private static CompleteMultipartUploadResult CompleteUploadPart(String bucketName, String objectName,
    String uploadId, List<PartETag> partETags,OssClient client)
        {
            var completeMultipartUploadRequest =
                new CompleteMultipartUploadRequest(bucketName, objectName, uploadId);
            foreach (var partETag in partETags)
            {
                completeMultipartUploadRequest.PartETags.Add(partETag);
            }

            return client.CompleteMultipartUpload(completeMultipartUploadRequest);
        }
        static void Main(string[] args)
        {
            if (args.Length != 5)
            {
                Console.WriteLine("Author:Jumbo");
                Console.WriteLine("Usage: ossupload.exe accessKeyId accessKeySecret bucketName endpoint fileToUpload");
            }
            else
            {

                string accessKeyId = args[0];
                string accessKeySecret = args[1];
                string bucketName = args[2];
                string endpoint = args[3];
                OssClient client = new OssClient(endpoint, accessKeyId, accessKeySecret);
                string fileToUpload = args[4];
                string key = Path.GetFileName(fileToUpload); //对象键
                UploadMultipart(bucketName, fileToUpload, key, client);

            }

        }
    }

}
