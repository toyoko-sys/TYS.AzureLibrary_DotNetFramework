using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using System;
using System.Threading.Tasks;

namespace TYS.AzureLibrary
{
    public class OperateQueueStorage
    {
        /// <summary>
        /// メッセージ追加
        /// </summary>
        /// <param name="storageAccount"></param>
        /// <param name="queueName"></param>
        /// <param name="message"></param>
        /// <param name="timeToLive"></param>
        /// <param name="initialVisibilityDelay"></param>
        /// <returns></returns>
        /// <remarks>
        /// Queueの有効期限を無期限(Fri, 31 Dec 9999 23:59:59 GMT)にしたい場合、timeToLiveに-1秒を設定
        /// デフォルトのQueueの有効期限は7日
        /// 非表示期間の指定は最大7日、それ以上を指定すると例外が発生する
        /// </remarks>
        public static async Task<bool> AddMessageAsync(CloudStorageAccount storageAccount, string queueName, string message, TimeSpan? timeToLive = null, TimeSpan? initialVisibilityDelay = null)
        {
            // queueへの参照を取得する
            var queue = GetQueueReference(storageAccount, queueName);

            // queueが存在しない場合作成する
            queue.CreateIfNotExists();

            // メッセージ作成
            CloudQueueMessage queueMessage = new CloudQueueMessage(message);
            // メッセージ追加
            await queue.AddMessageAsync(queueMessage, timeToLive, initialVisibilityDelay, null, null);

            return await Task.FromResult(true);
        }

        /// <summary>
        /// Queueへの参照
        /// </summary>
        /// <param name="storageAccount"></param>
        /// <param name="queueName"></param>
        /// <returns></returns>
        private static CloudQueue GetQueueReference(CloudStorageAccount storageAccount, string queueName)
        {
            // queue client を作成
            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();

            // 再試行ポリシーの構成
            QueueRequestOptions interactiveRequestOption = new QueueRequestOptions()
            {
                RetryPolicy = new LinearRetry(TimeSpan.FromSeconds(1), 5),
                //geo 冗長ストレージ(GRS)の場合、PrimaryThenSecondaryを設定する
                //それ以外は、PrimaryOnlyを設定する
                LocationMode = LocationMode.PrimaryOnly,
                MaximumExecutionTime = TimeSpan.FromSeconds(10)
            };
            queueClient.DefaultRequestOptions = interactiveRequestOption;

            // queue名に大文字は使えないので小文字に変換する
            queueName = System.Globalization.CultureInfo.CurrentCulture.TextInfo.ToLower(queueName);

            // queueへの参照を取得する
            return queueClient.GetQueueReference(queueName);
        }
    }
}
