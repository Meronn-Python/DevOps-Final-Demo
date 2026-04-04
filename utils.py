def calculate_total(price, tax_rate, discount):
    total = price + price * tax_rate - discount
    if total < 0:
        return 0
    else:
        return total


def format_message(prefix, value):
    return f"{prefix}: ${value: .2f}"
