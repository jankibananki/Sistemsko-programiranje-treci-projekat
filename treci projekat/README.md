# Sistemsko programiranje - treci projekat, zadatak 12

Konzolni web server koji koristi Rx.NET, Akka.NET, News API i SharpEntropy paket.

## Pokretanje

```bash
dotnet restore
set NEWS_API_KEY=YOUR_NEWS_API_KEY
dotnet run --project Projekat3
```

Primer zahteva:

```text
http://localhost:5000/?keyword=ai&category=technology
```

## Arhitektura

- `Program.cs` - pokretanje konzolnog servera.
- `WebServer.cs` - `HttpListener`, validacija, logovanje i prevod HTTP zahteva u poruke ka aktoru.
- `Article.cs` - periodican Rx.NET tok koji cita pracene upite iz aktora, poziva News API, filtrira i mapira `title` i `source`.
- `Result.cs` - Rx observer koji salje azurirane podatke aktoru.
- `NewsActors.cs` - Akka.NET aktor i poruke, interno stanje naslova po izvoru za svaki `keyword/category` upit.
- `TopicModeling.cs` - pojednostavljen topic modeling i SharpEntropy status.

Web server ne poziva eksterni API po korisnickom zahtevu. Korisnicki zahtev registruje upit i vraca trenutno stanje iz aktora, dok se stanje periodicki osvezava kroz Rx.NET tok u pozadini.
