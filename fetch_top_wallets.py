#!/usr/bin/env python3
"""
Fetch top 10k wallets from Dextra API and save to DataFrame using asyncio
"""

import asyncio
import aiohttp
import pandas as pd
from typing import List, Dict, Any


async def fetch_wallets_batch(session: aiohttp.ClientSession, offset: int = 0, limit: int = 200) -> Dict[str, Any]:
    """Fetch a batch of wallets from the API"""
    url = "https://dextradata.nftinit.io/api/hyper/get_wallets_profit_all_time/"
    
    params = {
        'period': 'all_time',
        'order': '-portfolio_perp_all_time_value',
        'offset': offset,
        'limit': limit,
        'max_first_time': '',
        'min_first_time': '',
        'max_end_time': '',
        'min_end_time': '',
        'max_short_kar': '',
        'min_short_kar': '',
        'max_long_kar': '',
        'min_long_kar': '',
        'max_pnl': '',
        'min_pnl': '',
        'max_uPnl': '',
        'min_uPnl': '',
        'max_rToken_count': '',
        'min_rToken_count': '',
        'max_uToken_count': '',
        'min_uToken_count': '',
        'max_perp_equity': '',
        'min_perp_equity': '',
        'max_margin_used': '',
        'min_margin_used': '',
        'max_margin_used_percentage': '',
        'min_margin_used_percentage': '',
        'max_avg_uLeverage_value': '',
        'min_avg_uLeverage_value': '',
        'max_funding': '',
        'min_funding': '',
        'max_margin_roi': '',
        'min_margin_roi': '',
        'max_rTx_count': '',
        'min_rTx_count': '',
        'max_sharpe': '',
        'min_sharpe': '',
        'max_growth_rate': '',
        'min_growth_rate': '',
        'max_perp_dd': '',
        'min_perp_dd': '',
        'max_perp_dd_pnl': '',
        'min_perp_dd_pnl': '',
        'max_win_complated_rate': '',
        'min_win_complated_rate': '',
        'max_complated_trades_count': '',
        'min_complated_trades_count': '',
        'max_complated_win_count': '',
        'min_complated_win_count': '',
        'max_complated_loss_count': '',
        'min_complated_loss_count': '',
        'user_token': '',
        'coin': '',
        'refcode': '',
        'is_favorite_wallet': ''
    }
    
    async with session.get(url, params=params) as response:
        response.raise_for_status()
        return await response.json()


async def fetch_top_wallets(target_count: int = 10000) -> pd.DataFrame:
    """Fetch top wallets and return as DataFrame using asyncio"""
    limit = 200
    total_batches = (target_count + limit - 1) // limit  # Ceiling division
    
    print(f"Fetching top {target_count} wallets in {total_batches} batches...")
    
    # Create connector with connection limits
    connector = aiohttp.TCPConnector(limit=10, limit_per_host=5)
    timeout = aiohttp.ClientTimeout(total=30)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # Create tasks for concurrent requests
        tasks = []
        for i in range(total_batches):
            offset = i * limit
            task = fetch_wallets_batch(session, offset=offset, limit=limit)
            tasks.append(task)
        
        # Execute all requests concurrently with progress tracking
        all_wallets = []
        completed = 0
        
        for coro in asyncio.as_completed(tasks):
            try:
                data = await coro
                results = data.get('results', [])
                all_wallets.extend(results)
                completed += 1
                print(f"Completed batch {completed}/{total_batches} - Total wallets: {len(all_wallets)}")
                
            except Exception as e:
                print(f"Error fetching batch: {e}")
                completed += 1
    
    # Sort by portfolio value and limit to target count
    all_wallets.sort(key=lambda x: float(x.get('portfolio_perp_all_time_value', 0)), reverse=True)
    all_wallets = all_wallets[:target_count]
    
    print(f"Successfully fetched {len(all_wallets)} wallets")
    
    # Convert to DataFrame
    df = pd.DataFrame(all_wallets)
    
    # Clean up the DataFrame - remove open_positions column as it's nested
    if 'open_positions' in df.columns:
        df = df.drop('open_positions', axis=1)
    
    return df


async def main():
    """Main async function"""
    try:
        # Fetch top 10k wallets
        df = await fetch_top_wallets(10000)
        
        # Save to CSV
        output_file = 'top_10k_wallets.csv'
        df.to_csv(output_file, index=False)
        print(f"Saved {len(df)} wallets to {output_file}")
        
        # Print basic info
        print(f"\nDataFrame shape: {df.shape}")
        print(f"\nColumns: {list(df.columns)}")
        
        # Print first few rows
        print(f"\nFirst 5 wallets:")
        print(df[['user_token', 'portfolio_perp_all_time_value', 'portfolio_perp_all_time_pnl']].head())
        
        # Print summary stats
        print(f"\nSummary statistics for portfolio value:")
        print(df['portfolio_perp_all_time_value'].astype(float).describe())
        
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())