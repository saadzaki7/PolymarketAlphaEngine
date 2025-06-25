# Polymarket Alpha Engine

A real-time arbitrage detection system for Polymarket prediction markets.

## Overview

This project provides tools to monitor Polymarket prediction markets in real-time, analyze orderbook data, and identify arbitrage opportunities across multi-outcome events. It connects to Polymarket's WebSocket API to receive live market data and performs continuous analysis to detect profitable trading opportunities.

## Key Features

- **Real-time Market Monitoring**: Connect to Polymarket's WebSocket API to receive live orderbook updates
- **Multi-Connection Management**: Handle large numbers of market subscriptions by distributing them across multiple WebSocket connections
- **Arbitrage Detection**: Identify arbitrage opportunities where the sum of best ask prices across all outcomes is less than a configurable threshold
- **Market Discovery**: Find active markets and their associated token IDs
- **Structured Data Models**: Well-defined data structures for orderbooks, events, and outcomes

## Components

- **WebSocket Clients**
  - `poly_websocket.py`: Core WebSocket client for connecting to Polymarket's API
  - `multi_poly_websocket.py`: Manager for handling multiple WebSocket connections

- **Data Processing**
  - `data_models.py`: Data structures for orderbooks, events, and outcomes
  - `parse_polymarket_events.py`: Parse raw event data from Polymarket
  - `filter_polymarket_events.py`: Filter events based on specific criteria

- **Analysis**
  - `arbitrage_analyzer.py`: Analyze events for arbitrage opportunities
  - `orderbook_manager.py`: Manage and update orderbooks for multiple markets
  - `order_printer.py`: Format and display orderbook data

- **Discovery**
  - `market_discovery.py`: Find active markets and their associated token IDs

## Architecture

The system follows a modular design:

1. **Data Collection**: WebSocket clients connect to Polymarket's API and receive real-time updates
2. **Data Processing**: Raw data is parsed and transformed into structured data models
3. **Analysis**: Processed data is analyzed for arbitrage opportunities
4. **Output**: Results are saved to files or displayed to the user

## Arbitrage Detection

The system identifies arbitrage opportunities by:

1. Monitoring the best ask prices across all outcomes in an event
2. Calculating the sum of these prices
3. Comparing the sum to a configurable threshold (default: 0.99)
4. If the sum is less than the threshold, an arbitrage opportunity exists

## File Descriptions

- `arbitrage_analyzer.py`: Core logic for detecting arbitrage opportunities
- `data_models.py`: Data structures for the nested orderbook components
- `filter_polymarket_events.py`: Filter events based on specific criteria
- `order_printer.py`: Format and display orderbook data
- `multi_poly_websocket.py`: Manage multiple WebSocket connections
- `orderbook_manager.py`: Manage and update orderbooks for multiple markets
- `parse_polymarket_events.py`: Parse raw event data from Polymarket
- `poly_websocket.py`: Core WebSocket client for connecting to Polymarket's API
- `market_discovery.py`: Find active markets and their associated token IDs

## Limitations and Considerations

- Polymarket's WebSocket API has a limit of approximately 480 token subscriptions per connection
- Arbitrage opportunities may exist for very short periods
- Market liquidity affects the feasibility of executing arbitrage trades
