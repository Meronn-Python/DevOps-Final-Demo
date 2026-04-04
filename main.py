from utils import calculate_total, format_message


def main():
    price = 100
    tax_rate = 0.13
    discount = 5
    total = calculate_total(price, tax_rate, discount)
    message = format_message("The final total is", total)
    print(message)


if __name__ == "__main__":
    main()
