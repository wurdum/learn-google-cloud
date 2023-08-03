using Google.Api.Gax.ResourceNames;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;

var projectId = "wurdum-project";
var projectName = ProjectName.FormatProject(projectId);

var topicId = "test-1";
var topicName = TopicName.FromProjectTopic(projectId, topicId);

var subscriptionId = topicId + "-sub";
var subscriptionName = SubscriptionName.FromProjectSubscription(projectId, subscriptionId);

var publisherServiceClient = PublisherServiceApiClient.Create();
var topics = publisherServiceClient.ListTopics(projectName);
var topic = topics.FirstOrDefault(t => t.TopicName == topicName);
if (topic == null)
{
    topic = publisherServiceClient.CreateTopic(topicName);
    Console.WriteLine($"Topic {topic.TopicName} created");
}

var subscriberServiceClient = SubscriberServiceApiClient.Create();
var subscriptions = subscriberServiceClient.ListSubscriptions(projectName);
var subscription = subscriptions.FirstOrDefault(s => s.TopicAsTopicName == topicName && s.SubscriptionName == subscriptionName);
if (subscription == null)
{
    subscription = subscriberServiceClient.CreateSubscription(subscriptionName, topicName, new(), 30);
    Console.WriteLine($"Subscription {subscription.SubscriptionName} created");
}

var messageId = "test1";
var client = PublisherClient.Create(topicName);
var message = new PubsubMessage
{
    MessageId = messageId,
    Data = ByteString.CopyFromUtf8("Hello World!!"),
    Attributes =
    {
        { "key1", "value1" },
        { "key2", "value2" }
    }
};

var id = await client.PublishAsync(message);
Console.WriteLine($"Message {messageId} published with id {id}");
