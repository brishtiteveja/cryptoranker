# import panel as pn
# import pandas as pd
# from pymongo import MongoClient
# from datetime import datetime, timedelta

# class CryptoDashboard:
#     def __init__(self):
#         self.client = MongoClient('mongodb://localhost:27017')
#         self.db = self.client.crypto_db
#         pn.extension('tabulator', sizing_mode="stretch_width", loading_spinner='dots')
        
#         self.page_size = 100
#         self.current_page = 1
#         self.sort_field = 'metrics.market_cap'
#         self.sort_dir = -1
#         self.total_pages = 1

#     def get_paginated_data(self, page=1, page_size=100, sort_field='metrics.market_cap', sort_dir=-1):
#         try:
#             skip = (page - 1) * page_size
            
#             pipeline = [
#                 # Group by coin_id and get the latest document
#                 {
#                     '$sort': {'timestamp': -1}
#                 },
#                 {
#                     '$group': {
#                         '_id': '$coin_id',
#                         'latest_doc': {'$first': '$$ROOT'}
#                     }
#                 },
#                 # Get the count before pagination
#                 {
#                     '$facet': {
#                         'total': [{'$count': 'count'}],
#                         'paginatedData': [
#                             {'$sort': {f'latest_doc.{sort_field}': sort_dir}},
#                             {'$skip': skip},
#                             {'$limit': page_size},
#                             {
#                                 '$project': {
#                                     '_id': 0,
#                                     'coin_id': '$_id',
#                                     'timestamp': '$latest_doc.timestamp',
#                                     'price': '$latest_doc.metrics.price',
#                                     'volume': '$latest_doc.metrics.volume',
#                                     'market_cap': '$latest_doc.metrics.market_cap',
#                                     'updated_at': '$latest_doc.updated_at'
#                                 }
#                             }
#                         ]
#                     }
#                 }
#             ]
            
#             result = list(self.db.historical_data.aggregate(pipeline))[0]
#             total_count = result['total'][0]['count'] if result['total'] else 0
#             data = pd.DataFrame(result['paginatedData'])
            
#             return {
#                 'data': data,
#                 'total_count': total_count,
#                 'page': page,
#                 'total_pages': (total_count + page_size - 1) // page_size
#             }
            
#         except Exception as e:
#             print(f"Error fetching data: {e}")
#             import traceback
#             traceback.print_exc()
#             return {
#                 'data': pd.DataFrame(),
#                 'total_count': 0,
#                 'page': page,
#                 'total_pages': 1
#             }

#     def create_dashboard(self):
#         template = pn.template.MaterialTemplate(
#             title='Crypto Analysis Dashboard',
#             sidebar_width=300
#         )
        
#         # Create loading indicator
#         loading_indicator = pn.indicators.LoadingSpinner(value=False, size=25, name='')
#         status_text = pn.pane.Markdown("Ready", styles={'color': 'green'})
        
#         # Create data table
#         table = pn.widgets.Tabulator(
#             pagination='remote',
#             page_size=self.page_size,
#             sizing_mode='stretch_width',
#             height=800,
#             loading=False,
#             configuration={
#                 'maxHeight': '800px',
#                 'columns': [
#                     {'title': 'Coin', 'field': 'coin_id', 'headerFilter': True},
#                     {
#                         'title': 'Price', 
#                         'field': 'price', 
#                         'formatter': 'money', 
#                         'formatterParams': {'precision': 8}
#                     },
#                     {
#                         'title': 'Volume', 
#                         'field': 'volume', 
#                         'formatter': 'money', 
#                         'formatterParams': {'precision': 2}
#                     },
#                     {
#                         'title': 'Market Cap', 
#                         'field': 'market_cap', 
#                         'formatter': 'money', 
#                         'formatterParams': {'precision': 2}
#                     },
#                     {
#                         'title': 'Last Updated',
#                         'field': 'timestamp',
#                         'formatter': 'datetime'
#                     }
#                 ]
#             }
#         )
        
#         page_number = pn.widgets.IntSlider(
#             name='Page',
#             start=1,
#             end=1,
#             value=1,
#             step=1,
#             width=100
#         )
        
#         prev_button = pn.widgets.Button(name='Previous', button_type='primary', width=100)
#         next_button = pn.widgets.Button(name='Next', button_type='primary', width=100)
        
#         def update_data(page):
#             # Show loading state
#             loading_indicator.value = True
#             status_text.object = "Loading data..."
#             prev_button.disabled = True
#             next_button.disabled = True
            
#             try:
#                 result = self.get_paginated_data(
#                     page=page,
#                     page_size=self.page_size,
#                     sort_field=self.sort_field,
#                     sort_dir=self.sort_dir
#                 )
                
#                 if not result['data'].empty:
#                     table.value = result['data']
#                     self.total_pages = result['total_pages']
#                     page_number.end = self.total_pages
                    
#                     # Update button states
#                     prev_button.disabled = page <= 1
#                     next_button.disabled = page >= self.total_pages
                    
#                     summary_metrics.object = f"""
#                     ### Market Summary
#                     Total Coins: {result['total_count']:,}
#                     Current Page: {page} of {self.total_pages}
#                     Showing {self.page_size} coins per page
#                     """
                    
#                     status_text.object = "Data loaded successfully"
#                 else:
#                     status_text.object = "No data available"
                    
#             except Exception as e:
#                 status_text.object = f"Error loading data: {str(e)}"
#                 print(f"Error: {str(e)}")
            
#             finally:
#                 # Hide loading state
#                 loading_indicator.value = False
            
#             return table
        
#         def on_prev_click(event):
#             if page_number.value > 1:
#                 page_number.value -= 1
#                 update_data(page_number.value)

#         def on_next_click(event):
#             if page_number.value < self.total_pages:
#                 page_number.value += 1
#                 update_data(page_number.value)
        
#         prev_button.on_click(on_prev_click)
#         next_button.on_click(on_next_click)
        
#         # Create summary metrics pane
#         summary_metrics = pn.pane.Markdown("### Loading...")
        
#         # Bind page selection to updates with debounce
#         page_number.param.watch(
#             lambda event: update_data(event.new), 
#             'value'
#         )
        
#         # Status bar layout
#         status_bar = pn.Row(
#             loading_indicator,
#             status_text,
#             css_classes=['status-bar']
#         )
        
#         # Layout
#         template.sidebar.append(
#             pn.Column(
#                 summary_metrics,
#                 pn.pane.Markdown("### Navigation"),
#                 pn.Row(prev_button, page_number, next_button),
#                 sizing_mode='stretch_width'
#             )
#         )
        
#         template.main.append(
#             pn.Column(
#                 status_bar,
#                 table,
#                 sizing_mode='stretch_width'
#             )
#         )
        
#         # Initial data load
#         update_data(1)
        
#         return template

#     def show_dashboard(self):
#         dashboard = self.create_dashboard()
#         dashboard.show()

# def main():
#     dashboard = CryptoDashboard()
#     dashboard.show_dashboard()

# if __name__ == '__main__':
#     main()




import panel as pn
import pandas as pd
from pymongo import MongoClient
from datetime import datetime, timedelta

class CryptoDashboard:
    def __init__(self):
        self.client = MongoClient('mongodb://localhost:27017')
        self.db = self.client.crypto_db
        pn.extension('tabulator', sizing_mode="stretch_width", loading_spinner='dots')
        
        self.page_size = 100
        self.current_page = 1
        self.sort_field = 'metrics.market_cap'
        self.sort_dir = -1
        self.total_pages = 1

    def get_paginated_data(self, page=1, page_size=100, sort_field='metrics.market_cap', sort_dir=-1):
        try:
            skip = (page - 1) * page_size
            
            pipeline = [
                # Group by coin_id and get the latest document
                {
                    '$sort': {'timestamp': -1}
                },
                {
                    '$group': {
                        '_id': '$coin_id',
                        'latest_doc': {'$first': '$$ROOT'}
                    }
                },
                # Get the count before pagination
                {
                    '$facet': {
                        'total': [{'$count': 'count'}],
                        'paginatedData': [
                            {'$sort': {f'latest_doc.{sort_field}': sort_dir}},
                            {'$skip': skip},
                            {'$limit': page_size},
                            {
                                '$project': {
                                    '_id': 0,
                                    'coin_id': '$_id',
                                    'timestamp': '$latest_doc.timestamp',
                                    'price': '$latest_doc.metrics.price',
                                    'volume': '$latest_doc.metrics.volume',
                                    'market_cap': '$latest_doc.metrics.market_cap',
                                    'updated_at': '$latest_doc.updated_at'
                                }
                            }
                        ]
                    }
                }
            ]
            
            result = list(self.db.historical_data.aggregate(pipeline))[0]
            total_count = result['total'][0]['count'] if result['total'] else 0
            data = pd.DataFrame(result['paginatedData'])
            
            return {
                'data': data,
                'total_count': total_count,
                'page': page,
                'total_pages': (total_count + page_size - 1) // page_size
            }
            
        except Exception as e:
            print(f"Error fetching data: {e}")
            import traceback
            traceback.print_exc()
            return {
                'data': pd.DataFrame(),
                'total_count': 0,
                'page': page,
                'total_pages': 1
            }

    def create_dashboard(self):
        template = pn.template.MaterialTemplate(
            title='Crypto Analysis Dashboard',
            sidebar_width=300
        )
        
        # Create loading indicator
        loading_indicator = pn.indicators.LoadingSpinner(value=False, size=25, name='')
        status_text = pn.pane.Markdown("Ready", styles={'color': 'green'})
        
        # Create data table
        table = pn.widgets.Tabulator(
            pagination='remote',
            page_size=self.page_size,
            sizing_mode='stretch_width',
            height=800,
            loading=False,
            configuration={
                'maxHeight': '800px',
                'columns': [
                    {'title': 'Coin', 'field': 'coin_id', 'headerFilter': True},
                    {
                        'title': 'Price', 
                        'field': 'price', 
                        'formatter': 'money', 
                        'formatterParams': {'precision': 8}
                    },
                    {
                        'title': 'Volume', 
                        'field': 'volume', 
                        'formatter': 'money', 
                        'formatterParams': {'precision': 2}
                    },
                    {
                        'title': 'Market Cap', 
                        'field': 'market_cap', 
                        'formatter': 'money', 
                        'formatterParams': {'precision': 2}
                    },
                    {
                        'title': 'Last Updated',
                        'field': 'timestamp',
                        'formatter': 'datetime'
                    }
                ]
            }
        )
        
        page_number = pn.widgets.IntSlider(
            name='Page',
            start=1,
            end=1,
            value=1,
            step=1,
            width=100
        )
        
        prev_button = pn.widgets.Button(name='Previous', button_type='primary', width=100)
        next_button = pn.widgets.Button(name='Next', button_type='primary', width=100)
        
        def update_data(page):
            # Show loading state
            loading_indicator.value = True
            status_text.object = "Loading data..."
            prev_button.disabled = True
            next_button.disabled = True
            
            try:
                result = self.get_paginated_data(
                    page=page,
                    page_size=self.page_size,
                    sort_field=self.sort_field,
                    sort_dir=self.sort_dir
                )
                
                if not result['data'].empty:
                    table.value = result['data']
                    self.total_pages = result['total_pages']
                    page_number.end = self.total_pages
                    
                    # Update button states
                    prev_button.disabled = page <= 1
                    next_button.disabled = page >= self.total_pages
                    
                    start = (page - 1) * self.page_size + 1
                    end = min(page * self.page_size, result['total_count'])
                    summary_metrics.object = f"""
                    ### Market Summary
                    Total Coins: {result['total_count']:,}
                    Current Page: {page} of {self.total_pages} (Showing {start} - {end})
                    Showing {self.page_size} coins per page
                    """
                    
                    status_text.object = "Data loaded successfully"
                else:
                    status_text.object = "No data available"
                    
            except Exception as e:
                status_text.object = f"Error loading data: {str(e)}"
                print(f"Error: {str(e)}")
            
            finally:
                # Hide loading state
                loading_indicator.value = False
            
            return table
        
        def on_prev_click(event):
            if page_number.value > 1:
                page_number.value -= 1
                update_data(page_number.value)

        def on_next_click(event):
            if page_number.value < self.total_pages:
                page_number.value += 1
                update_data(page_number.value)
        
        prev_button.on_click(on_prev_click)
        next_button.on_click(on_next_click)
        
        # Create summary metrics pane
        summary_metrics = pn.pane.Markdown("### Loading...")
        
        # Bind page selection to updates with debounce
        page_number.param.watch(
            lambda event: update_data(event.new), 
            'value'
        )
        
        # Status bar layout
        status_bar = pn.Row(
            loading_indicator,
            status_text,
            css_classes=['status-bar']
        )
        
        # Layout
        template.sidebar.append(
            pn.Column(
                summary_metrics,
                pn.pane.Markdown("### Navigation"),
                pn.Row(prev_button, page_number, next_button),
                sizing_mode='stretch_width'
            )
        )
        
        template.main.append(
            pn.Column(
                status_bar,
                table,
                sizing_mode='stretch_width'
            )
        )
        
        # Initial data load
        update_data(1)
        
        return template

    def show_dashboard(self):
        dashboard = self.create_dashboard()
        dashboard.show()

def main():
    dashboard = CryptoDashboard()
    dashboard.show_dashboard()

if __name__ == '__main__':
    main()