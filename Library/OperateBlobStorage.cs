using Microsoft.WindowsAzure.Storage;
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
        /// <param name="storageAccount"></param>
        /// <param name="containerName"></param>
        /// <param name="blobName"></param>
        /// <param name="stream"></param>
        /// <param name="standardBlobTier"></param>
        /// <param name="shouldBlobDelete"></param>
        /// <returns></returns>
        /// <remarks>
        /// 層を指定してアップロードすることが出来ない。
        /// アップロード後に層を設定する or コンテナの既定の層を変更する(Azure Portal)
        /// 本メソッドによる方法は、既定の層に置いてある時間分のコスト＋層移動のコストがかかるので注意
        /// </remarks>
        public static async Task<bool> UploadStreamAsync(CloudStorageAccount storageAccount, string containerName, string blobName, Stream stream, StandardBlobTier standardBlobTier = StandardBlobTier.Unknown, bool shouldBlobDelete = false)
        {
            // blobコンテナへの参照を取得する
            var container = GetContainerReference(storageAccount, containerName);

            // コンテナが存在しない場合作成する
            container.CreateIfNotExists();

            // コンテナからblobブロックの参照を取得する
            // フォルダ階層ありのアップロードを行う場合、blobNameを「folder/image.jpg」のようにする
            CloudBlockBlob blockBlob = container.GetBlockBlobReference(blobName);

            // 指定があれば既存のBlobを削除
            if (shouldBlobDelete)
            {
                await blockBlob.DeleteIfExistsAsync();
            }

            // シーク位置を初期位置に戻す
            stream.Seek(0, SeekOrigin.Begin);

            // ファイルをアップロードする
            await blockBlob.UploadFromStreamAsync(stream);

            // 指定があれば層を設定
            if (standardBlobTier != StandardBlobTier.Unknown)
            {
                await blockBlob.SetStandardBlobTierAsync(standardBlobTier);
            }

            return await Task.FromResult(true);
        }

        /// <summary>
        /// テキストのアップロード
        /// </summary>
        /// <param name="storageAccount"></param>
        /// <param name="containerName"></param>
        /// <param name="blobName"></param>
        /// <param name="uploadText"></param>
        /// <param name="standardBlobTier"></param>
        /// <param name="shouldBlobDelete"></param>
        /// <returns></returns>
        /// <remarks>
        /// </remarks>
        public static async Task<bool> UploadTextAsync(CloudStorageAccount storageAccount, string containerName, string blobName, string uploadText, StandardBlobTier standardBlobTier = StandardBlobTier.Unknown, bool shouldBlobDelete = false) {
            // blobコンテナへの参照を取得する
            var container = GetContainerReference(storageAccount, containerName);

            // コンテナが存在しない場合作成する
            container.CreateIfNotExists();

            // コンテナからblobブロックの参照を取得する
            CloudBlockBlob blockBlob = container.GetBlockBlobReference(blobName);

            // 指定があれば既存のBlobを削除
            if (shouldBlobDelete) {
                await blockBlob.DeleteIfExistsAsync();
            }

            // テキストをアップロードする
            await blockBlob.UploadTextAsync(uploadText);

            // 指定があれば層を設定
            if (standardBlobTier != StandardBlobTier.Unknown) {
                await blockBlob.SetStandardBlobTierAsync(standardBlobTier);
            }

            return await Task.FromResult(true);
        }

        /// <summary>
        /// ダウンロード
        /// </summary>
        /// <param name="storageAccount"></param>
        /// <param name="containerName"></param>
        /// <param name="blobName"></param>
        /// <returns></returns>
        public static async Task<MemoryStream> DownloadStreamAsync(CloudStorageAccount storageAccount, string containerName, string blobName)
        {
            // blobコンテナへの参照を取得する
            var container = GetContainerReference(storageAccount, containerName);

            // コンテナからblobブロックの参照を取得する
            CloudBlockBlob blockBlob = container.GetBlockBlobReference(blobName);

            var stream = new MemoryStream();
            await blockBlob.DownloadToStreamAsync(stream);
            stream.Seek(0, SeekOrigin.Begin);

            return await Task.FromResult(stream);
        }

        /// <summary>
        /// テキストのダウンロード
        /// </summary>
        /// <param name="storageAccount"></param>
        /// <param name="containerName"></param>
        /// <param name="blobName"></param>
        /// <returns></returns>
        public static async Task<string> DownloadTextAsync(CloudStorageAccount storageAccount, string containerName, string blobName) {
            // blobコンテナへの参照を取得する
            var container = GetContainerReference(storageAccount, containerName);

            // コンテナからblobブロックの参照を取得する
            CloudBlockBlob blockBlob = container.GetBlockBlobReference(blobName);

            string downloadText = await blockBlob.DownloadTextAsync();

            return await Task.FromResult(downloadText);
        }

        /// <summary>
        /// 層変更
        /// </summary>
        /// <param name="storageAccount"></param>
        /// <param name="containerName"></param>
        /// <param name="blobName"></param>
        /// <param name="standardBlobTier"></param>
        /// <returns></returns>
        public static async Task<bool> SetStandardBlobTierAsync(CloudStorageAccount storageAccount, string containerName, string blobName, StandardBlobTier standardBlobTier)
        {
            // blobコンテナへの参照を取得する
            var container = GetContainerReference(storageAccount, containerName);

            // コンテナからblobブロックの参照を取得する
            CloudBlockBlob blockBlob = container.GetBlockBlobReference(blobName);

            // 層変更
            await blockBlob.SetStandardBlobTierAsync(standardBlobTier);

            return await Task.FromResult(true);
        }

        /// <summary>
        /// プロパティ取得
        /// </summary>
        /// <param name="storageAccount"></param>
        /// <param name="containerName"></param>
        /// <param name="blobName"></param>
        /// <returns></returns>
        public static async Task<BlobProperties> GetBlobPropertiesAsync(CloudStorageAccount storageAccount, string containerName, string blobName)
        {
            // blobコンテナへの参照を取得する
            var container = GetContainerReference(storageAccount, containerName);

            // コンテナからblobブロックの参照を取得する
            CloudBlockBlob blockBlob = container.GetBlockBlobReference(blobName);

            await blockBlob.FetchAttributesAsync();

            return await Task.FromResult(blockBlob.Properties);
        }

        /// <summary>
        /// コンテナ存在チェック
        /// </summary>
        /// <param name="storageAccount"></param>
        /// <param name="containerName"></param>
        /// <returns></returns>
        public static async Task<bool> ContainerExistsAsync(CloudStorageAccount storageAccount, string containerName)
        {
            // blobコンテナへの参照を取得する
            var container = GetContainerReference(storageAccount, containerName);

            // blobコンテナの存在チェック
            var containerExists = await container.ExistsAsync();

            return await Task.FromResult(containerExists);
        }

        /// <summary>
        /// Blobブロック存在チェック
        /// </summary>
        /// <param name="storageAccount"></param>
        /// <param name="containerName"></param>
        /// <param name="blobName"></param>
        /// <returns></returns>
        public static async Task<bool> BlockBlobExistsAsync(CloudStorageAccount storageAccount, string containerName, string blobName)
        {
            // blobコンテナへの参照を取得する
            var container = GetContainerReference(storageAccount, containerName);

            // コンテナからblobブロックの参照を取得する
            CloudBlockBlob blockBlob = container.GetBlockBlobReference(blobName);

            // blobブロックの存在チェック
            var blockBlobExists = await blockBlob.ExistsAsync();

            return await Task.FromResult(blockBlobExists);
        }

        /// <summary>
        /// Blob名の接頭辞検索（提供されている機能による制約）
        /// </summary>
        /// <param name="storageAccount"></param>
        /// <param name="containerName"></param>
        /// <param name="prefix"></param>
        /// <returns></returns>
        public static List<string> SearchBlobName(CloudStorageAccount storageAccount, string containerName, string prefix)
        {
            // blobコンテナへの参照を取得する
            var container = GetContainerReference(storageAccount, containerName);

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
        /// <param name="storageAccount"></param>
        /// <param name="containerName"></param>
        /// <returns></returns>
        private static CloudBlobContainer GetContainerReference(CloudStorageAccount storageAccount, string containerName)
        {
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
