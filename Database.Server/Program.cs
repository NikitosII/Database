using OwnDatabase;
using Database.Core.Interfaces;
using Database.Server.Controllers;

var builder = WebApplication.CreateBuilder(args);

var dbPath = builder.Configuration.GetValue<string>("DatabasePath") ?? "cows.odb";
builder.Services.AddSingleton<ICowDatabase>(new CowDatabase(dbPath));
builder.Services.AddControllers();

var app = builder.Build();
app.MapControllers();
app.Run();
