-- name: create-category
create table if not exists category
(
    id         serial primary key,
    source_id  bigint,
    name       text,
    created_at date default current_date
);

-- name: create-product
create table if not exists product
(
    id               serial primary key,
    source_id        bigint,
    category_id      int,
    title            text,
    price_unit       int,
    price_sale       int,
    url              text,
    brand            text,
    rating           smallint,
    reviews_quantity int,
    specs_id         int,
    created_at       date default current_date
);

-- name: create-specs
create table if not exists specs
(
    id         serial primary key,
    product_id bigint,
    type       text,
    cpu        text,
    hertz      text,
    cores      text,
    gpu        text,
    ram        text,
    ram_type   text,
    ssd        text,
    drive_size text,
    camera     text,
    battery    text,
    extra      jsonb,
    created_at date default current_date
);

-- name: create-review
create table if not exists review
(
    id              serial primary key,
    source_id       bigint,
    product_id      bigint,
    date            date,
    rating          float,
    comment_plus    text,
    comment_minus   text,
    comment_text    text,
    review_approved text,
    review_rated    text,
    created_at      date default current_date
);