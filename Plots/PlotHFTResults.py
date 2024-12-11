import pandas as pd
import plotly.graph_objects as go


# Step 1: Read CSV file
def read_csv_file(file_path):
    df = pd.read_csv(file_path, header=None, names=[
        'datetime', 'bot_id', 'side', 'ma_entry_spread', 'entry_spread',
        'ma_exit_spread', 'exit_spread', 'limit_order', 'fr_factor',
        'entry_bound', 'exit_bound', 'impact_bid_price_okx',
        'impact_ask_price_binance', 'buy_spread_ma', 'sell_spread_ma', 'buy_spread_sd', 'sell_spead_sd', 'okx_ob', 'bin_ob', 'impact_reached'
    ])
    return df

# Step 2: Process and filter the data
def process_data(df):
    df['datetime'] = pd.to_datetime(df['datetime'], format='mixed')

    # Filter out rows where entry_bound or exit_bound is 1 or -1
    df = df[(df['entry_bound'] != 1) & (df['entry_bound'] != -1) &
            (df['exit_bound'] != 1) & (df['exit_bound'] != -1)]

    # Select every 3rd point
    df = df.iloc[::3].reset_index(drop=True)

    # Identify points to highlight
    df['highlight_entry'] = df['entry_spread'] > df['entry_bound']
    df['highlight_exit'] = df['exit_spread'] < df['exit_bound']

    return df


# Step 3: Create the plot
def create_plot(df, filename):
    fig = go.Figure()

    # Plot entry and exit spreads
    fig.add_trace(go.Scatter(x=df['datetime'], y=df['entry_spread'], name='Sell Spread', line=dict(color='#778DA9')))
    fig.add_trace(go.Scatter(x=df['datetime'], y=df['exit_spread'], name='Buy Spread', line=dict(color='#415A77')))


    # Plot entry and exit bounds
    fig.add_trace(go.Scatter(x=df['datetime'], y=df['entry_bound'], name='Sell Bound', line=dict(color='#0D1B2A')))
    fig.add_trace(go.Scatter(x=df['datetime'], y=df['exit_bound'], name='Buy Bound', line=dict(color='#1B263B')))

    # Highlight points where entry spread is above entry bound
    fig.add_trace(go.Scatter(
        x=df[df['highlight_entry']]['datetime'],
        y=df[df['highlight_entry']]['entry_spread'],
        mode='markers',
        name='Entry Spread > Entry Bound',
        marker=dict(color='red', size=5, symbol='circle')
    ))

    # Highlight points where exit spread is below exit bound
    fig.add_trace(go.Scatter(
        x=df[df['highlight_exit']]['datetime'],
        y=df[df['highlight_exit']]['exit_spread'],
        mode='markers',
        name='Exit Spread < Exit Bound',
        marker=dict(color='green', size=5, symbol='circle')
    ))

    fig.update_layout(
        height=600,
        title_text=F"Trade Analysis: Bounds and Spreads ({filename})",
        xaxis_title="Time",
        yaxis_title="Value",
        legend_title="Metrics",
        hovermode="x unified"
    )

    return fig

# Main execution
if __name__ == "__main__":
    # Get the filename from the user
    symbols = ['POPCAT', 'APE', 'MASK', 'NEIRO', 'HMSTR', 'BIGTIME', 'UXLINK', 'DOGS', 'TRX', 'TURBO', 'MEW', 'JTO', 'MKR', 'TIA', 'AAVE', 'OM', 'AR','TRB', 'ORBS', 'AUCTION', 'ZETA', 'SUI', 'STORJ', 'ENJ']
    # symbols = ['MEW']
    for coin in symbols:
        filename = f'{coin}_20241210.csv'
        linux_path = f"/home/ec2-user/TradeLogs/20241210/{filename}"

        # Read and process the data
        df = read_csv_file(linux_path)
        df = process_data(df)
        # Create and show the plot
        fig = create_plot(df, filename)
        fig.show()