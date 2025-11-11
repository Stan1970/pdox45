from django import template
register = template.Library()

@register.filter
def get_value(post, col):
    return post.get(f'value_{col}', '')

@register.filter
def get_op(post, col):
    return post.get(f'operator_{col}', '')

@register.filter
def get_summary(post, col):
    return post.get(f'summary_operator_{col}', '')

@register.filter
def get_select(post, col):
    return post.get(f'select_{col}', '')
