class APILimitExceeded:
    
    # 120 pages equivalent to 30,000 releases (120*250)
    def __init__(self, page_threshold=120):
        
        self.page_threshold = page_threshold 

    def generate_user_notice(self, interaction_type):

        notice_prefix = f'Wow! You have a lot of items in your {interaction_type}.'
        reason = f'My limit is 30,000 items for a private {interaction_type}.'
        imperative = f"Please make your {interaction_type} temporarily public so that I can gather it quicker."
        return f'{notice_prefix} {reason} {imperative}'