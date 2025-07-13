using DSMessageBroker.Services;

namespace MessageBroker.Storage
{
    public interface IStorageEngine
    {
        Task AppendAsync(Message message);
        Task<IEnumerable<Message>> RecoverAsync();
    }
}
