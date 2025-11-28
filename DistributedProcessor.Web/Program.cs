var builder = WebApplication.CreateBuilder(args);

var app = builder.Build();

app.UseDefaultFiles();  // Serve index.html by default
app.UseStaticFiles();   // Serve static files from wwwroot

app.Run();
