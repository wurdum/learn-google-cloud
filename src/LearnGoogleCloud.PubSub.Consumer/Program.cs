using Google.Api.Gax;
using Google.Api.Gax.Grpc;
using Google.Api.Gax.ResourceNames;
using Google.Cloud.PubSub.V1;
using Grpc.Core;

var projectId = "wurdum-project";
var projectName = ProjectName.FormatProject(projectId);

var topicId = "test-1";
var topicName = TopicName.FromProjectTopic(projectId, topicId);

var subscriptionId = "test-1-sub";
var subscriptionName = SubscriptionName.FromProjectSubscription(projectId, subscriptionId);

var client = SubscriberServiceApiClient.Create();
var cts = new CancellationTokenSource();
var pull = client.StreamingPull(CallSettings.FromCancellationToken(cts.Token), new(10));

await pull.TryWriteAsync(new()
{
    SubscriptionAsSubscriptionName = subscriptionName,
    StreamAckDeadlineSeconds = 20
});

_ = Task.Run(async () =>
{
    await pull.TryWriteAsync(new()
    {
        SubscriptionAsSubscriptionName = subscriptionName,
        ClientId = "1"
    });

    await foreach (var response in pull.GetResponseStream())
    {
        var acks = new List<string>();
        foreach (var message in response.ReceivedMessages)
        {
            Console.WriteLine($"Consumed message {message.Message.MessageId} with data {message.Message.Data.ToStringUtf8()}");
            client.Acknowledge(subscriptionName, new[] { message.AckId });
            acks.Add(message.AckId);
        }

        if (acks.Count > 0)
        {
            await pull.TryWriteAsync(new()
            {
                AckIds = { acks }
            });
        }
    }
});

Console.ReadLine();
cts.Cancel();



/*try
{
    var response = client.Pull(subscriptionName, 10, settings);
    foreach (var message in response.ReceivedMessages)
    {
        Console.WriteLine($"Consumed message {message.Message.MessageId} with data {message.Message.Data.ToStringUtf8()}");
        client.Acknowledge(subscriptionName, new[] { message.AckId });
    }
}
catch (RpcException exception) when (exception.StatusCode == StatusCode.DeadlineExceeded)
{
    Console.WriteLine("No messages to consume!");
}*/
