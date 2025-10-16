from django import template
register = template.Library()

@register.filter
def get_value(post, col):
    return post.get(f'value_{col}', '')
