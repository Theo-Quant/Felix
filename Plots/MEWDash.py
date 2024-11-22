import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import pandas as pd
import plotly.graph_objs as go
import os
from flask_caching import Cache

# Initialize the Dash app
app = dash.Dash(__name__)

# Setup caching
cache = Cache(app.server, config={
    'CACHE_TYPE': 'filesystem',
    'CACHE_DIR': 'cache-directory'
})
TIMEOUT = 30  # cache timeout in seconds

@cache.memoize(timeout=TIMEOUT)
def read_csv_file(coin):
    filename = f'bot_{coin}_20241121_V3.5.csv'
    linux_path = f"/home/ec2-user/TradeLogs/{filename}"

    if not os.path.exists(linux_path):
        print(f"File not found: {linux_path}")
        return pd.DataFrame()

    try:
        df = pd.read_csv(linux_path, header=None, names=[
            'datetime', 'bot_id', 'side', 'ma_entry_spread', 'entry_spread',
            'ma_exit_spread', 'exit_spread', 'limit_order', 'fr_factor',
            'entry_bound', 'exit_bound', 'impact_bid_price_okx',
            'impact_ask_price_binance', 'buy_spread_ma', 'sell_spread_ma', 'buy_spread_sd', 'sell_spead_sd', 'okx_ob',
            'bin_ob', 'impact_reached'
        ])

        df['datetime'] = pd.to_datetime(df['datetime'], format='mixed')
        df = df.sort_values('datetime').tail(20000)  # Last N rows
        df = df.iloc[::10]  # Every N point

        return df
    except Exception as e:
        print(f"Error reading file {linux_path}: {e}")
        return pd.DataFrame()

# Layout of the app
app.layout = html.Div([
    html.H1('OKX Coin Trading Dashboard'),
    html.Div([
        dcc.Input(id='coin-input', type='text', placeholder='Enter coin name (e.g., BTC)', value='AUCTION'),
        html.Button('Update', id='update-button', n_clicks=0)
    ]),
    dcc.Graph(id='coin-graph', style={'height': '90vh'}),
    dcc.Interval(
        id='interval-component',
        interval=5 * 1000,  # Update every 5 seconds
        n_intervals=0
    )
])

@app.callback(
    Output('coin-graph', 'figure'),
    [Input('update-button', 'n_clicks'),
     Input('interval-component', 'n_intervals')],
    [State('coin-input', 'value')]
)
def update_graph(n_clicks, n_intervals, coin):
    if not coin:
        return go.Figure()

    df = read_csv_file(coin.upper())

    if df.empty:
        fig = go.Figure()
        fig.add_annotation(text=f"No data available for {coin.upper()}",
                           xref="paper", yref="paper",
                           x=0.5, y=0.5, showarrow=False)
        return fig

    fig = go.Figure()

    fig.add_trace(go.Scatter(x=df['datetime'], y=df['entry_spread'], mode='lines', name='Sell Spread',
                             line=dict(color='blue')))
    fig.add_trace(go.Scatter(x=df['datetime'], y=df['exit_spread'], mode='lines', name='Buy Spread',
                             line=dict(color='red')))
    fig.add_trace(go.Scatter(x=df['datetime'], y=df['entry_bound'], mode='lines', name='Sell Bound',
                             line=dict(color='green')))
    fig.add_trace(go.Scatter(x=df['datetime'], y=df['exit_bound'], mode='lines', name='Buy Bound',
                             line=dict(color='orange')))

    fig.update_layout(
        title=f"{coin.upper()} Spreads and Bounds",
        xaxis_title="DateTime",
        yaxis_title="Spread/Bound",
        height=800,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )

    return fig

if __name__ == '__main__':
    app.run_server(debug=True, port=8050)