import datetime

from pydantic import BaseModel, Json


class Category(BaseModel):
    source_id: int
    name: str

    class Config:
        orm_mode = True


class Product(BaseModel):
    source_id: int
    category_id: int
    title: str
    price_unit: int
    price_sale: int
    url: str
    brand: str = None
    rating: float
    reviews_quantity: int

    class Config:
        orm_mode = True


class Specs(BaseModel):
    product_id: int
    type: str = None
    cpu: str = None
    hertz: int = None
    cores: int = None
    gpu: str = None
    ram: str = None
    ram_type: str = None
    ssd: int = None
    battery: str = None
    extra: Json

    class Config:
        orm_mode = True


class Review(BaseModel):
    product_id: int
    source_id: int
    date: datetime.date = None
    rating: float
    comment_plus: str = None
    comment_minus: str = None
    comment_text: str = None
    review_approved: int = None
    review_rated: int = None

    class Config:
        orm_mode = True
