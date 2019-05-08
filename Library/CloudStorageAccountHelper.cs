using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;

namespace TYS.AzureLibrary
{
    /// <summary>
    /// CloudStorageAccountを取得するためのヘルパークラス
    /// </summary>
    public class CloudStorageAccountHelper
    {
        public static CloudStorageAccount Get(string connectionString)
        {
            return CloudStorageAccount.Parse(connectionString);
        }

        public static CloudStorageAccount Get(string accountName, string accountKey)
        {
            return new CloudStorageAccount(new StorageCredentials(accountName, accountKey), true);
        }
    }
}
