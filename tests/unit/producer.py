import pytest
from unittest.mock import Mock, patch
from producer.producer import StockDataProducer

class TestStockDataProducer:
    
    @pytest.fixture
    def producer(self):
        with patch.dict('os.environ', {
            'RAPIDAPI_KEY': 'test_key',
            'RAPIDAPI_HOST': 'test.host.com',
            'STOCK_SYMBOLS': 'AAPL,MSFT'
        }):
            return StockDataProducer()
    
    def test_fetch_stock_data_success(self, producer):
        mock_response = Mock()
        mock_response.json.return_value = {
            'data': {
                'price': 150.25,
                'volume': 1000000,
                'name': 'Apple Inc',
                'sector': 'Technology'
            }
        }
        
        with patch('requests.get', return_value=mock_response):
            data = producer.fetch_stock_data('AAPL')
            assert data['symbol'] == 'AAPL'
            assert data['price'] == 150.25
            assert data['company_name'] == 'Apple Inc'
    
    def test_fetch_stock_data_failure(self, producer):
        with patch('requests.get', side_effect=Exception('API Error')):
            with pytest.raises(Exception):
                producer.fetch_stock_data('AAPL')