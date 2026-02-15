import os
import stripe

stripe.api_key = os.getenv("STRIPE_SECRET_KEY")

STRIPE_PRICE_ID = os.getenv("STRIPE_PRICE_ID")
SUCCESS_URL = os.getenv(
    "STRIPE_SUCCESS_URL",
    "http://localhost:8000/datasentinel/dashboard"
)
CANCEL_URL = os.getenv(
    "STRIPE_CANCEL_URL",
    "http://localhost:8000/datasentinel"
)

def create_checkout_session(customer_email: str) -> str:
    """
    Creates a Stripe Checkout session and returns the redirect URL
    """

    if not STRIPE_PRICE_ID:
        raise RuntimeError("STRIPE_PRICE_ID not set")

    session = stripe.checkout.Session.create(
        payment_method_types=["card"],
        mode="subscription",
        customer_email=customer_email,
        line_items=[
            {
                "price": STRIPE_PRICE_ID,
                "quantity": 1,
            }
        ],
        success_url=SUCCESS_URL,
        cancel_url=CANCEL_URL,
    )

    return session.url
