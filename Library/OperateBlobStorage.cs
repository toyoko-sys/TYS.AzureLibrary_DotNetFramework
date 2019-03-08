﻿using Microsoft.WindowsAzure.Storage;
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
        /// <remarks>
        /// 層を指定してアップロードすることが出来ない。
        /// アップロード後に層を設定する or コンテナの既定の層を変更する(Azure Portal)
        /// 本メソッドによる方法は、既定の層に置いてある時間分のコスト＋層移動のコストがかかるので注意
        /// </remarks>
        public static async Task<bool> UploadStreamAsync(string accountName, string accountKey, string containerName, string fileName, Stream fileStream, StandardBlobTier standardBlobTier = StandardBlobTier.Unknown)
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

            // 指定があれば層を設定
            if (standardBlobTier != StandardBlobTier.Unknown)
            {
                await blockBlob.SetStandardBlobTierAsync(standardBlobTier);
            }

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
        public static async Task<Stream> DownloadFileAsync(string accountName, string accountKey, string containerName, string fileName)
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
        /// 層変更
        /// </summary>
        /// <param name="accountName"></param>
        /// <param name="accountKey"></param>
        /// <param name="containerName"></param>
        /// <param name="fileName"></param>
        /// <param name="standardBlobTier"></param>
        /// <returns></returns>
        public static async Task<bool> SetStandardBlobTierAsync(string accountName, string accountKey, string containerName, string fileName, StandardBlobTier standardBlobTier)
        {
            // blobコンテナへの参照を取得する
            var container = GetContainerReference(accountName, accountKey, containerName);

            // コンテナからblobブロックの参照を取得する
            CloudBlockBlob blockBlob = container.GetBlockBlobReference(fileName);

            // 層変更
            await blockBlob.SetStandardBlobTierAsync(standardBlobTier);

            return await Task.FromResult(true);
        }

        /// <summary>
        /// プロパティ取得
        /// </summary>
        /// <param name="accountName"></param>
        /// <param name="accountKey"></param>
        /// <param name="containerName"></param>
        /// <param name="fileName"></param>
        /// <returns></returns>
        public static async Task<BlobProperties> GetBlobPropertiesAsync(string accountName, string accountKey, string containerName, string fileName)
        {
            // blobコンテナへの参照を取得する
            var container = GetContainerReference(accountName, accountKey, containerName);

            // コンテナからblobブロックの参照を取得する
            CloudBlockBlob blockBlob = container.GetBlockBlobReference(fileName);

            await blockBlob.FetchAttributesAsync();

            return await Task.FromResult(blockBlob.Properties);
        }

        /// <summary>
        /// コンテナ存在チェック
        /// </summary>
        /// <param name="accountName"></param>
        /// <param name="accountKey"></param>
        /// <param name="containerName"></param>
        /// <returns></returns>
        public static async Task<bool> ContainerExistsAsync(string accountName, string accountKey, string containerName)
        {
            // blobコンテナへの参照を取得する
            var container = GetContainerReference(accountName, accountKey, containerName);

            // blobコンテナの存在チェック
            var containerExists = await container.ExistsAsync();

            return await Task.FromResult(containerExists);
        }

        /// <summary>
        /// Blobブロック存在チェック
        /// </summary>
        /// <param name="accountName"></param>
        /// <param name="accountKey"></param>
        /// <param name="containerName"></param>
        /// <param name="blobName"></param>
        /// <returns></returns>
        public static async Task<bool> BlockBlobExistsAsync(string accountName, string accountKey, string containerName, string blobName)
        {
            // blobコンテナへの参照を取得する
            var container = GetContainerReference(accountName, accountKey, containerName);

            // コンテナからblobブロックの参照を取得する
            CloudBlockBlob blockBlob = container.GetBlockBlobReference(blobName);

            // blobブロックの存在チェック
            var blockBlobExists = await blockBlob.ExistsAsync();

            return await Task.FromResult(blockBlobExists);
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

        /// <summary>
        /// Blobコンテナへの参照
        /// </summary>
        /// <param name="accountName"></param>
        /// <param name="accountKey"></param>
        /// <param name="containerName"></param>
        /// <returns></returns>
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
