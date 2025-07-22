using DSMessageBroker.Services;

namespace DSMessageBroker.Tests
{
    public class ConsumerDeduplicatorTests
    {
        [Fact]
        public void Tracks_And_Detects_Duplicates()
        {
            var dedup = new ConsumerDeduplicator();
            var id = Guid.NewGuid();

            Assert.False(dedup.IsDuplicate(id));

            dedup.MarkProcessed(id);

            Assert.True(dedup.IsDuplicate(id));
        }

        [Fact]
        public void Handles_Multiple_Ids()
        {
            var dedup = new ConsumerDeduplicator();
            var ids = Enumerable.Range(0, 1000).Select(_ => Guid.NewGuid()).ToList();

            foreach (var id in ids)
                dedup.MarkProcessed(id);

            foreach (var id in ids)
                Assert.True(dedup.IsDuplicate(id));
        }
    }
}
