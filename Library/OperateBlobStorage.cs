using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace TYS.AzureLibrary
{
    /// <summary>
    /// Blobストレージ操作
    /// </summary>
    public class OperateBlobStorage
    {
        /// <summary>
        /// アップロード
        /// </summary>
        /// <param name="accountName"></param>
        /// <param name="accountKey"></param>
        /// <param name="containerName"></param>
        /// <param name="fileName"></param>
        /// <param name="fileStream"></param>
        /// <returns></returns>
        public static async Task<bool> UploadFileToStorage(string accountName, string accountKey, string containerName, string fileName, Stream fileStream)
        {
            // blobコンテナへの参照を取得する
            var container = GetContainerReference(accountName, accountKey, containerName);

            // コンテナが存在しない場合作成する
            container.CreateIfNotExists();

            // コンテナからblobブロックの参照を取得する
            // フォルダ階層ありのアップロードを行う場合、blobNameを「folder/image.jpg」のようにする
            CloudBlockBlob blockBlob = container.GetBlockBlobReference(fileName);

            // ファイルをアップロードする
            await blockBlob.UploadFromStreamAsync(fileStream);

            return await Task.FromResult(true);
        }

        /// <summary>
        /// ダウンロード
        /// </summary>
        /// <param name="accountName"></param>
        /// <param name="accountKey"></param>
        /// <param name="containerName"></param>
        /// <param name="fileName"></param>
        /// <returns></returns>
        public static async Task<Stream> DownloadFileFromStorage(string accountName, string accountKey, string containerName, string fileName)
        {
            // blobコンテナへの参照を取得する
            var container = GetContainerReference(accountName, accountKey, containerName);

            // コンテナからblobブロックの参照を取得する
            CloudBlockBlob blockBlob = container.GetBlockBlobReference(fileName);

            var stream = new MemoryStream();
            await blockBlob.DownloadToStreamAsync(stream);

            return await Task.FromResult((Stream)stream);
        }

        /// <summary>
        /// コンテナ存在チェック
        /// </summary>
        /// <param name="accountName"></param>
        /// <param name="accountKey"></param>
        /// <param name="containerName"></param>
        /// <returns></returns>
        public static async Task<bool> ContainerExists(string accountName, string accountKey, string containerName)
        {
            // blobコンテナへの参照を取得する
            var container = GetContainerReference(accountName, accountKey, containerName);

            // blobコンテナの存在チェック
            var containerExists = await container.ExistsAsync();
            return containerExists;
        }

        /// <summary>
        /// Blobブロック存在チェック
        /// </summary>
        /// <param name="accountName"></param>
        /// <param name="accountKey"></param>
        /// <param name="containerName"></param>
        /// <param name="blobName"></param>
        /// <returns></returns>
        public static async Task<bool> BlockBlobExists(string accountName, string accountKey, string containerName, string blobName)
        {
            // blobコンテナへの参照を取得する
            var container = GetContainerReference(accountName, accountKey, containerName);

            // コンテナからblobブロックの参照を取得する
            CloudBlockBlob blockBlob = container.GetBlockBlobReference(blobName);

            // blobブロックの存在チェック
            var blockBlobExists = await blockBlob.ExistsAsync();

            return blockBlobExists;
        }

        /// <summary>
        /// Blob名の接頭辞検索（提供されている機能による制約）
        /// </summary>
        /// <param name="storageConfig"></param>
        /// <param name="containerName"></param>
        /// <param name="prefix"></param>
        /// <returns></returns>
        public static List<string> SearchBlobName(string accountName, string accountKey, string containerName, string prefix)
        {
            // blobコンテナへの参照を取得する
            var container = GetContainerReference(accountName, accountKey, containerName);

            // blobコンテナ内でprefixと部分一致するリストを取得する
            var blobList = container.ListBlobs(prefix, true);
            if (!blobList.Any())
            {
                return null;
            }

            List<string> blobNameList = new List<string>();
            foreach (var blobItem in blobList)
            {
                if (blobItem.GetType() == typeof(CloudBlockBlob))
                {
                    CloudBlockBlob blob = (CloudBlockBlob)blobItem;
                    blobNameList.Add(blob.Name);
                }
            }
            return blobNameList;
        }

        private static CloudBlobContainer GetContainerReference(string accountName, string accountKey, string containerName)
        {
            // storagecredentials オブジェクトを作成する
            StorageCredentials storageCredentials = new StorageCredentials(accountName, accountKey);

            // ストレージの資格情報を渡して、cloudstorage account を作成する
            CloudStorageAccount storageAccount = new CloudStorageAccount(storageCredentials, true);

            // blob client を作成
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            // 再試行ポリシーの構成
            BlobRequestOptions interactiveRequestOption = new BlobRequestOptions()
            {
                RetryPolicy = new LinearRetry(TimeSpan.FromSeconds(1), 5),
                //geo 冗長ストレージ(GRS)の場合、PrimaryThenSecondaryを設定する
                //それ以外は、PrimaryOnlyを設定する
                LocationMode = LocationMode.PrimaryOnly,
                MaximumExecutionTime = TimeSpan.FromSeconds(10)
            };
            blobClient.DefaultRequestOptions = interactiveRequestOption;

            // コンテナ名に大文字は使えないので小文字に変換する
            containerName = System.Globalization.CultureInfo.CurrentCulture.TextInfo.ToLower(containerName);

            // blobコンテナへの参照を取得する
            return blobClient.GetContainerReference(containerName);
        }
    }
}
