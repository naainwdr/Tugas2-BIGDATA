from fastapi import FastAPI, HTTPException
from mongo_utils import get_data_from_mongo, get_available_tickers
from fastapi.responses import JSONResponse, HTMLResponse
import asyncio
import concurrent.futures

app = FastAPI(title="Stock Aggregation API", version="1.0")

@app.get("/stock/{ticker}/{interval}")
def read_data(ticker: str, interval: str):
    data = get_data_from_mongo(ticker.upper(), interval)
    if not data:
        return JSONResponse(
            status_code=404,
            content={"status": "error", "message": f"Data tidak ditemukan untuk {ticker} ({interval})", "data": []}
        )
    return {"status": "success", "data": data}

@app.get("/stock/all/{interval}")
def get_all_data(interval: str):
    all_data = {}
    tickers = get_available_tickers()
    for ticker in tickers:
        data = get_data_from_mongo(ticker, interval)
        if data:
            all_data[ticker] = data
    return {"status": "success", "data": all_data}

@app.post("/stock/refresh/{ticker}")
async def refresh_data(ticker: str):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(executor, run_aggregation_for_multiple_tickers, ticker.upper())
    return {"status": "success", "message": f"Data untuk {ticker} telah diperbarui"}

@app.get("/visualize/all", response_class=HTMLResponse)
async def visualize_all():
    tickers = get_available_tickers()

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Visualisasi Saham</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            .chart-container {{ height: 400px; margin-bottom: 40px; }}
            .controls {{ margin-bottom: 20px; }}
        </style>
    </head>
    <body>
        <h1>Visualisasi Semua Ticker</h1>
        <div class="controls">
            <label for="interval">Pilih Interval: </label>
            <select id="interval" onchange="drawCharts()">
                <option value="daily">Daily</option>
                <option value="monthly" selected>Monthly</option>
                <option value="yearly">Yearly</option>
            </select>
        </div>
        <div id="charts"></div>

        <script>
        const tickers = {tickers};

        async function fetchData(ticker, interval) {{
            const response = await fetch(`/stock/${{ticker}}/${{interval}}`);
            const result = await response.json();
            return result.status === 'success' ? result.data : [];
        }}

        async function drawCharts() {{
            const interval = document.getElementById("interval").value;
            const chartsDiv = document.getElementById("charts");
            chartsDiv.innerHTML = "";

            for (let ticker of tickers) {{
                const data = await fetchData(ticker, interval);
                if (data.length === 0) continue;

                const labels = data.map(item =>
                    new Date(item.Date || item.Month || item.Year).toLocaleDateString()
                );
                const openData = data.map(item => item.Avg_Open);
                const closeData = data.map(item => item.Avg_Close);

                const container = document.createElement("div");
                container.className = "chart-container";
                container.innerHTML = `<h2>${{ticker}}</h2><canvas id="chart-${{ticker}}"></canvas>`;
                chartsDiv.appendChild(container);

                const ctx = document.getElementById(`chart-${{ticker}}`).getContext('2d');
                new Chart(ctx, {{
                    type: 'line',
                    data: {{
                        labels: labels,
                        datasets: [
                            {{
                                label: 'Avg Open',
                                data: openData,
                                borderColor: 'rgba(75, 192, 192, 1)',
                                backgroundColor: 'rgba(75, 192, 192, 0.2)',
                                tension: 0.1
                            }},
                            {{
                                label: 'Avg Close',
                                data: closeData,
                                borderColor: 'rgba(153, 102, 255, 1)',
                                backgroundColor: 'rgba(153, 102, 255, 0.2)',
                                tension: 0.1
                            }}
                        ]
                    }},
                    options: {{
                        responsive: true,
                        maintainAspectRatio: false,
                        scales: {{
                            y: {{ beginAtZero: false }}
                        }}
                    }}
                }});
            }}
        }}

        drawCharts();
        </script>
    </body>
    </html>
    """
    return html