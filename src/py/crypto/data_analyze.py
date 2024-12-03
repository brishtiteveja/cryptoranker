import pandas as pd

crypto_data = '/Users/andy/Desktop/'
file_name = 'Perf_1Dec2024_2024-12-01.csv'

# import streamlit as st
import pandas as pd
# import plotly.express as px

def display_analysis_streamlit(results):
    st.set_page_config(layout="wide")
    st.title('Crypto Analysis Results')
    
    for category, df in results.items():
        st.header(category.replace('_', ' ').title())
        
        # Create two columns
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # Interactive table
            st.dataframe(
                df,
                column_config={
                    col: st.column_config.NumberColumn(format="%.2f")
                    for col in df.select_dtypes('float64').columns
                },
                hide_index=True,
            )
        
        with col2:
            # Add a relevant visualization
            if 'Performance % 1 month' in df.columns:
                fig = px.bar(
                    df.head(10),
                    x='Coin',
                    y='Performance % 1 month',
                    title='Top 10 Monthly Performers'
                )
                st.plotly_chart(fig)

import panel as pn
pn.extension()

def display_analysis_panel_primitive(results):
    dashboard = pn.Column()
    dashboard.append(pn.pane.Markdown('# Crypto Analysis Results'))
    
    for category, df in results.items():
        dashboard.append(pn.pane.Markdown(f"## {category.replace('_', ' ').title()}"))
        dashboard.append(pn.widgets.DataFrame(df, sizing_mode='stretch_width'))
    
    return dashboard

def create_searchable_table(df, name):
    """Create a table with advanced search capabilities"""
    
    # Create search input
    search_input = pn.widgets.TextInput(
        name='Quick Search',
        placeholder=f'Search in {name}...',
        width=300
    )
    
    # Create table
    table = pn.widgets.Tabulator(
        df,
        pagination='remote',
        page_size=15,
        sizing_mode='stretch_width',
        header_filters=True,
        show_index=False,
        configuration={
            'layout': 'fitColumns',
            'tooltips': True,
            'selectable': 1,  # Allow selecting one row
            'filterMode': 'input',
            'headerFilter': True,
            # Add highlighting for searched term
            'rowFormatter': """
                function(row) {
                    var searchTerm = document.querySelector('input[name="Quick Search"]').value;
                    if (searchTerm && row.getData().Coin.toLowerCase().includes(searchTerm.toLowerCase())) {
                        row.getElement().style.backgroundColor = "#fff3cd";
                    }
                }
            """
        }
    )
    
    def filter_table(event):
        """Filter table based on search input"""
        search_term = event.new.lower()
        if search_term:
            filtered_df = df[df['Coin'].str.lower().str.contains(search_term, na=False)]
            table.value = filtered_df
        else:
            table.value = df
    
    search_input.param.watch(filter_table, 'value')
    
    return pn.Column(
        pn.Row(search_input),
        table
    )

def display_analysis_panel(results):
    pn.extension('tabulator')
    
    # Create sidebar
    sidebar = pn.Column(width=250)
    sidebar.append(pn.pane.Markdown('### Global Search'))
    
    # Global search input
    global_search = pn.widgets.TextInput(
        name='Search Any Table',
        placeholder='Enter coin name...',
        width=200
    )
    sidebar.append(global_search)
    
    # Create tabs
    tabs = pn.Tabs()
    
    # Add each analysis with searchable table
    for category, df in results.items():
        display_name = category.replace('_', ' ').title()
        
        # Create searchable table component
        searchable_table = create_searchable_table(df, display_name)
        
        # Add to tabs
        tabs.append((
            display_name,
            pn.Column(
                pn.pane.Markdown(f"## {display_name}"),
                searchable_table
            )
        ))
        
        sidebar.append(
            pn.widgets.Button(
                name=display_name,
                button_type='light',
                width=200
            )
        )
    
    template = pn.template.MaterialTemplate(
        title='Crypto Analysis Dashboard',
        sidebar=sidebar,
        main=pn.Column(
            pn.Row(
                pn.pane.Markdown('# Crypto Analysis Results'),
                sizing_mode='stretch_width'
            ),
            tabs,
            sizing_mode='stretch_width'
        ),
        sidebar_width=300
    )
    
    return template

def read_data():
    data = pd.read_csv(crypto_data + file_name)
    return data

def analyze_data(data):
    # Identify tokens with positive trends across multiple timeframes
    momentum_df = data[
        (data['Performance % 1 week'] > 0) &
        (data['Performance % 1 month'] > 0) &
        (data['Performance % 3 months'] > 0)
    ]
    print(momentum_df[['Coin', 'Performance % 1 week', 'Performance % 1 month', 'Performance % 3 months']])

    # Find tokens with the highest and lowest volatility
    top_volatile = data.sort_values(by='Volatility 1 day', ascending=False).head(10)
    low_volatile = data.sort_values(by='Volatility 1 day', ascending=True).head(10)

    print("Top Volatile Coins:", top_volatile[['Coin', 'Volatility 1 day']])
    print("Least Volatile Coins:", low_volatile[['Coin', 'Volatility 1 day']])
    
    
    # Identify tokens with high transaction volume and 24-hour volume
    active_tokens = data.sort_values(by='Volume in USD 24 hours', ascending=False).head(10)
    print(active_tokens[['Coin', 'Volume in USD 24 hours', 'Transaction volume in USD']])
    
    # Compare long-term performance (5 years, 10 years) with short-term performance
    survivors_df = data[
        (data['Performance % 5 years'] > 0) &
        (data['Performance % 10 years'] > 0) &
        (data['Performance % 1 week'] > 0)
    ]
    print(survivors_df[['Coin', 'Performance % 5 years', 'Performance % 10 years', 'Performance % 1 week']])

    # Identify tokens with extreme negative returns or erratic behavior
    red_flags = data[
        (data['Performance % All Time'] < -90) &
        (data['Performance % 1 month'] < 0) &
        (data['Market capitalization'] < 1e7)
    ]
    print(red_flags[['Coin', 'Performance % All Time', 'Performance % 1 month', 'Market capitalization']])

    # Identify assets with significant drops but recent recovery
    recovery_df = data[
        (data['Performance % All Time'] < -90) &  # Large historical drop
        (data['Performance % 1 month'] > 0) &    # Recent recovery in 1 month
        (data['Performance % 1 week'] > 0)       # Recent recovery in 1 week
    ]
    print(recovery_df[['Coin', 'Performance % All Time', 'Performance % 1 month', 'Performance % 1 week']])



    # import matplotlib.pyplot as plt

    # # Short-term vs Long-term performance scatter plot
    # plt.scatter(data['Performance % 1 week'], data['Performance % 6 months'])
    # plt.title('Short-Term vs Long-Term Performance')
    # plt.xlabel('Performance % 1 week')
    # plt.ylabel('Performance % 6 months')
    # plt.show()

    # # Market Cap vs Performance scatter plot
    # plt.scatter(data['Market capitalization'], data['Performance % 1 month'])
    # plt.title('Market Cap vs 1-Month Performance')
    # plt.xlabel('Market Capitalization')
    # plt.ylabel('Performance % 1 month')
    # plt.xscale('log')  # Use log scale for better visualization of market caps
    # plt.show()
    
    # import seaborn as sns

    # # Correlation heatmap for performance metrics
    # performance_columns = [
    #     'Performance % 1 week', 'Performance % 1 month', 
    #     'Performance % 3 months', 'Performance % 6 months'
    # ]
    # correlation_matrix = data[performance_columns].corr()

    # sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm')
    # plt.title('Correlation Between Performance Metrics')
    # plt.show()


        
    # Do some analysis
    return data

def identify_recovery_momentum_leaders(data, thresholds):
    """
    Find coins showing consistent positive performance across multiple timeframes
    """
    momentum_leaders = data[
        (data['Price Change % 24 hours'] > thresholds['24h']) &
        (data['Performance % 1 week'] > thresholds['1w']) &
        (data['Performance % 1 month'] > thresholds['1m']) &
        (data['Performance % 3 months'] > thresholds['3m']) &
        (data['Market capitalization'] > thresholds['min_market_cap'])
    ]

    return momentum_leaders[['Coin', 'Price Change % 24 hours', 'Performance % 1 week', 
                           'Performance % 1 month', 'Performance % 3 months', 'Market capitalization']]

def identify_early_stage_breakouts(data, thresholds):
    """
    Find coins showing recent strong performance but still below ATH
    """
    breakouts = data[
        (data['Performance % 1 month'] > thresholds['1m']) &
        (data['Performance % 3 months'] > thresholds['3m']) &
        (data['Performance % All Time'] < thresholds['max_ath_drop']) &
        (data['Market capitalization'] > thresholds['min_market_cap'])
    ]
    return breakouts[['Coin', 'Performance % 1 month', 'Performance % 3 months', 
                     'Performance % All Time', 'Market capitalization']]

def identify_established_performers(data, thresholds):
    """
    Find coins with steady growth and substantial all-time returns
    """
    established = data[
        (data['Performance % All Time'] > thresholds['min_ath']) &
        (data['Performance % 1 month'] > thresholds['1m']) &
        (data['Performance % 3 months'] > thresholds['3m']) &
        (data['Market capitalization'] > thresholds['min_market_cap'])
    ]
    return established[['Coin', 'Performance % 1 month', 'Performance % 3 months', 
                       'Performance % All Time', 'Market capitalization']]

def identify_volatile_high_risk(data, thresholds):
    """
    Find coins with extreme percentage swings
    """
    volatile = data[
        (data['Volatility 1 day'] > thresholds['volatility']) &
        (abs(data['Performance % 1 month']) > thresholds['1m_change']) &
        (data['Market capitalization'] > thresholds['min_market_cap'])
    ]
    return volatile[['Coin', 'Volatility 1 day', 'Performance % 1 month', 
                    'Price Change % 24 hours', 'Market capitalization']]

def identify_recovery_phase(data, thresholds):
    """
    Find coins showing recovery after significant drops
    """
    recovery = data[
        (data['Performance % All Time'] < thresholds['max_ath_drop']) &
        (data['Performance % 1 month'] > thresholds['1m']) &
        (data['Performance % 1 week'] > thresholds['1w']) &
        (data['Market capitalization'] > thresholds['min_market_cap'])
    ]
    return recovery[['Coin', 'Performance % All Time', 'Performance % 1 month', 
                    'Performance % 1 week', 'Market capitalization']]

def identify_consistent_performers(data, thresholds):
    """
    Find coins with steady, less volatile growth
    """
    consistent = data[
        (data['Volatility 1 day'] < thresholds['max_volatility']) &
        (data['Performance % 1 month'] > thresholds['1m']) &
        (data['Performance % 3 months'] > thresholds['3m']) &
        (data['Market capitalization'] > thresholds['min_market_cap'])
    ]
    return consistent[['Coin', 'Volatility 1 day', 'Performance % 1 month', 
                      'Performance % 3 months', 'Market capitalization']]

def analyze_trends(data):
    # Define thresholds for each analysis
    thresholds = {
        '24h': 5,           # 5% minimum 24h return
        '1w': 10,          # 10% minimum weekly return
        '1m': 30,          # 30% minimum monthly return
        '3m': 50,          # 50% minimum quarterly return
        'min_market_cap': 10000000,  # $10M minimum market cap
        'max_ath_drop': -50,         # Maximum 50% drop from ATH
        'min_ath': 1000,             # Minimum 1000% all-time return
        'volatility': 5,             # Minimum daily volatility
        '1m_change': 100,            # Minimum 100% monthly change
        'max_volatility': 3          # Maximum daily volatility for consistent performers
    }
    
    # Run all analyses
    results = {
        'momentum_leaders': identify_recovery_momentum_leaders(data, thresholds),
        'early_breakouts': identify_early_stage_breakouts(data, thresholds),
        'established': identify_established_performers(data, thresholds),
        'volatile': identify_volatile_high_risk(data, thresholds),
        'recovery': identify_recovery_phase(data, thresholds),
        'consistent': identify_consistent_performers(data, thresholds)
    }
    
    # Print results
    for category, df in results.items():
        print(f"\n{category.upper()} ({len(df)} coins):")
        print(df.head())
        
    return results

def analyze_trends(data):
    # Define thresholds for each analysis
    thresholds = {
        '24h': 5,           # 5% minimum 24h return
        '1w': 10,          # 10% minimum weekly return
        '1m': 30,          # 30% minimum monthly return
        '3m': 50,          # 50% minimum quarterly return
        'min_market_cap': 10000000,  # $10M minimum market cap
        'max_ath_drop': -50,         # Maximum 50% drop from ATH
        'min_ath': 1000,             # Minimum 1000% all-time return
        'volatility': 5,             # Minimum daily volatility
        '1m_change': 100,            # Minimum 100% monthly change
        'max_volatility': 3          # Maximum daily volatility for consistent performers
    }
    
    # Run all analyses
    results = {
        'momentum_leaders': identify_recovery_momentum_leaders(data, thresholds),
        'early_breakouts': identify_early_stage_breakouts(data, thresholds),
        'established': identify_established_performers(data, thresholds),
        'volatile': identify_volatile_high_risk(data, thresholds),
        'recovery': identify_recovery_phase(data, thresholds),
        'consistent': identify_consistent_performers(data, thresholds)
    }
    
    # Print results
    for category, df in results.items():
        print(f"\n{category.upper()} ({len(df)} coins):")
        print(df.head())
        
    return results

def calculate_ath_metrics(data):
    """
    Calculate ATH metrics using performance data
    Returns DataFrame with ATH-related calculations
    """
    df = data.copy()
    
    # Current price is our base
    current_price = df['Price']
    
    # Calculate potential ATH using different timeframes
    ath_from_alltime = current_price / (1 + (df['Performance % All Time'] / 100))
    ath_from_5yr = current_price / (1 + (df['Performance % 5 years'] / 100))
    ath_from_10yr = current_price / (1 + (df['Performance % 10 years'] / 100))
    
    # Take the maximum as our best estimate of ATH
    df['Estimated ATH'] = pd.concat([
        ath_from_alltime,
        ath_from_5yr,
        ath_from_10yr
    ], axis=1).max(axis=1)
    
    # Calculate distance from ATH as percentage
    df['Distance from ATH %'] = ((df['Estimated ATH'] - df['Price']) / df['Estimated ATH'] * 100).round(2)
    
    return df

def find_near_ath_coins(data, threshold_percentage=20):
    """
    Find coins that are within specified percentage of their ATH
    """
    # Calculate ATH metrics
    df = calculate_ath_metrics(data)
    
    # Filter coins near ATH
    near_ath = df[
        # Must be within threshold of ATH
        (df['Distance from ATH %'] <= threshold_percentage) &
        # Add minimum market cap filter
        (df['Market capitalization'] > 10000000) &
        # Add minimum volume filter
        (df['Volume in USD 24 hours'] > 100000)
    ].copy()
    
    # Sort by distance from ATH
    near_ath = near_ath.sort_values('Distance from ATH %')
    
    # Select relevant columns
    result = near_ath[[
        'Coin',
        'Price',
        'Estimated ATH',
        'Distance from ATH %',
        'Price Change % 24 hours',
        'Performance % 1 week',
        'Performance % 1 month',
        'Market capitalization',
        'Volume in USD 24 hours'
    ]]
    
    return result

def find_breakout_candidates(data, ath_threshold=30, momentum_threshold=10):
    """
    Find coins that might be attempting to break ATH
    """
    # Calculate ATH metrics
    df = calculate_ath_metrics(data)
    
    candidates = df[
        # Within threshold of ATH
        (df['Distance from ATH %'] <= ath_threshold) &
        # Strong recent performance
        (df['Performance % 1 week'] > momentum_threshold) &
        (df['Performance % 1 month'] > momentum_threshold * 2) &
        # Good volume
        (df['Volume in USD 24 hours'] > df['Market capitalization'] * 0.01) &
        # Minimum market cap
        (df['Market capitalization'] > 10000000)
    ].copy()
    
    candidates['Volume/Market Cap %'] = (candidates['Volume in USD 24 hours'] / candidates['Market capitalization'] * 100).round(2)
    
    return candidates.sort_values('Distance from ATH %')[[
        'Coin',
        'Price',
        'Estimated ATH',
        'Distance from ATH %',
        'Performance % 1 week',
        'Performance % 1 month',
        'Volume/Market Cap %',
        'Market capitalization'
    ]]

# Example usage:
def analyze_near_ath(data):
    
    # Find coins within different ranges of ATH
    within_10_percent = find_near_ath_coins(data, 10)
    within_20_percent = find_near_ath_coins(data, 20)
    within_30_percent = find_near_ath_coins(data, 30)
    
    print("\nCoins within 10% of ATH:")
    print(within_10_percent)
    
    print("\nCoins within 20% of ATH:")
    print(within_20_percent)
    
    print("\nCoins within 30% of ATH:")
    print(within_30_percent)
    
    # Additional analysis: Find coins with strong momentum near ATH
    strong_momentum = within_30_percent[
        (within_30_percent['Performance % 1 week'] > 5) &
        (within_30_percent['Performance % 1 month'] > 15)
    ]
    
    print("\nCoins near ATH with strong momentum:")
    print(strong_momentum)
    
    return {
        'within_10_percent': within_10_percent,
        'within_20_percent': within_20_percent,
        'within_30_percent': within_30_percent,
        'strong_momentum': strong_momentum
    }

def main():
    data = read_data()
    
    #analyze_data(data)
    
    results = analyze_trends(data)
    
    results['near_ath_10'] = find_near_ath_coins(data, 10)
    results['near_ath_20'] = find_near_ath_coins(data, 20)
    results['near_ath_30'] = find_near_ath_coins(data, 30)
    results['ath_breakout_candidates'] = find_breakout_candidates(data)
    
    dashboard = display_analysis_panel(results)
    dashboard.show()
 
if __name__ == '__main__':
    main()


