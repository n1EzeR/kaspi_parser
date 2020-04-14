-- name: bulk-insert-categories
insert into category(name, source_id)
VALUES %s;

-- name: bulk-insert-products
insert into product(source_id, category_id, title,
                    price_unit, price_sale, url,
                    brand, rating, reviews_quantity)
VALUES (:source_id, :category_id, :title,
        :price_unit, :price_sale, :url,
        :brand, :rating, :reviews_quantity);

-- name: bulk-insert-reviews
insert into review(source_id, product_id, date,
                   rating, comment_plus, comment_minus,
                   comment_text, review_approved, review_rated)
VALUES (:source_id, :product_id, :date,
        :rating, :comment_plus, :comment_minus,
        :comment_text, :review_approved, :review_rated);

-- name: bulk-insert-specs
insert into specs(product_id, type, cpu, hertz, cores, gpu, ram,
                  ssd, drive_size, camera, battery, extra)
VALUES (:product_id, :type, :cpu, :hertz, :cores, :gpu, :ram,
        :ssd, :drive_size, :camera, :battery, :extra);
