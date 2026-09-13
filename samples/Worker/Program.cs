using Microsoft.Extensions.Hosting;
using RedisNearCache;
using RedisNearCache.Samples.Worker;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddRedisNearCache("localhost:6379");
builder.Services.AddHostedService<ConfigPollingWorker>();

var host = builder.Build();

Console.WriteLine("RedisNearCache Worker sample starting.");
Console.WriteLine("It polls the key 'config:feature-flags' every 500 ms through the near cache.");
Console.WriteLine("From another terminal, change the watched key and watch the very next tick pick it up as a miss:");
Console.WriteLine("""  docker exec redis-near-cache-redis redis-cli SET config:feature-flags '{"beta":true}'""");
Console.WriteLine();

await host.RunAsync();
