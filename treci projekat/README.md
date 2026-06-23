# Sistemsko programiranje - treći projekat, zadatak 12

Konzolni web server koji koristi Rx.NET, Akka.NET, News API i SharpEntropy paket.

## Pokretanje

```bash
dotnet restore
dotnet run --project Projekat3
```

Primer zahteva:

```text
http://localhost:5000/?keyword=ai&category=technology&apiKey=YOUR_NEWS_API_KEY
```

API ključ može i kroz environment promenljivu:

```bash
set NEWS_API_KEY=YOUR_NEWS_API_KEY
```

## Struktura

- `Program.cs` - pokretanje konzolnog servera
- `WebServer.cs` - HttpListener, validacija, logovanje, prevod HTTP zahteva u poruke ka aktoru
- `Article.cs` - Rx.NET tok koji poziva News API i mapira `title` i `source`
- `Result.cs` - Rx observer koji šalje poruke aktoru
- `NewsActors.cs` - Akka.NET aktor i poruke, interno stanje naslova po izvoru
- `TopicModeling.cs` - pojednostavljen topic modeling i SharpEntropy status
