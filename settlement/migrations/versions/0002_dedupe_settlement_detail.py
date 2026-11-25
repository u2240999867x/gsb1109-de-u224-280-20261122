"""结算明细去重：每个供应商、每个规范单号只保留一条

Revision ID: 0002
Revises: 0001
Create Date: 2026-11-26

供应商重传结算明细时同一笔用了不同大小写的单号，收货行已经引用了
各次入库拿到的明细 ID。本迁移用 SQLite batch 重建把历史数据清成
每个供应商、每个规范单号（去首尾空格并转大写）只有一条：

* 同组的金额（十进制文本精确转整数分）或数量（整数字符串转整数）
  不一致时，守卫表的 INSERT 触发主键冲突，整次迁移以 SQL 错误中止，
  换表和版本登记都不会发生；冲突明细可用 PLAN.conflict_query() 另查。
* 能合并时保留最小的明细 ID 和那条的原始金额文本，组内所有收货行
  改挂到保留的 ID，收货行自己的主键和内容照旧。
* 新表保证规范单号在供应商内唯一，total_cents 仍由数据库根据
  金额分和数量计算，收货表外键的删除级联保留。

迁移连接全程开启外键检查（见 env.py），因此重建分三步：先拆掉
收货表外键（避免 DROP 旧明细表时级联删掉收货行），再重建明细表，
最后把带 ON DELETE CASCADE 的外键建回来。
"""

from alembic import op
import sqlalchemy as sa

from settlement.dedupe import DedupePlan, decimal_text_to_cents

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


# 怎样从旧表取值、怎样认定同一笔、哪些原始行不能合并。
PLAN = DedupePlan(
    table="settlement_detail",
    id_column="id",
    group_columns={
        "supplier_code": "{t}.supplier_code",
        "order_no": "upper(trim({t}.order_no))",
    },
    value_columns={
        "amount_cents": decimal_text_to_cents("{t}.amount_text"),
        "quantity": "CAST(trim({t}.quantity_text) AS INTEGER)",
    },
    writeback={
        "order_no": "upper(trim({t}.order_no))",
    },
    children=(("receipt_line", "detail_id"),),
)

_meta = sa.MetaData()

# 两张旧表的定义，供 batch 迁移的 copy_from 使用（离线 --sql 不需要反射）。
old_settlement_detail = sa.Table(
    "settlement_detail",
    _meta,
    sa.Column("id", sa.Integer(), primary_key=True),
    sa.Column("supplier_code", sa.Text(), nullable=False),
    sa.Column("order_no", sa.Text(), nullable=False),
    sa.Column("amount_text", sa.Text(), nullable=False),
    sa.Column("quantity_text", sa.Text(), nullable=False),
)

old_receipt_line = sa.Table(
    "receipt_line",
    _meta,
    sa.Column("id", sa.Integer(), primary_key=True),
    sa.Column("detail_id", sa.Integer(), nullable=False),
    sa.Column("product_code", sa.Text(), nullable=False),
    sa.Column("received_qty", sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(
        ["detail_id"],
        ["settlement_detail.id"],
        name="fk_receipt_line_detail_id",
        ondelete="CASCADE",
    ),
)

AMOUNT_CENTS_SQL = decimal_text_to_cents("amount_text")
TOTAL_CENTS_SQL = "amount_cents * quantity"


def _rebuild_receipt_line_without_fk():
    with op.batch_alter_table(
        "receipt_line", copy_from=old_receipt_line, recreate="always"
    ) as batch_op:
        batch_op.drop_constraint(
            "fk_receipt_line_detail_id", type_="foreignkey"
        )


def _rebuild_receipt_line_with_fk():
    with op.batch_alter_table(
        "receipt_line", copy_from=old_receipt_line, recreate="always"
    ) as batch_op:
        pass


def upgrade():
    for statement in PLAN.data_statements():
        op.execute(sa.text(statement))

    _rebuild_receipt_line_without_fk()

    with op.batch_alter_table(
        "settlement_detail", copy_from=old_settlement_detail, recreate="always"
    ) as batch_op:
        batch_op.alter_column(
            "quantity_text",
            new_column_name="quantity",
            type_=sa.Integer(),
            existing_type=sa.Text(),
        )
        batch_op.add_column(
            sa.Column(
                "amount_cents",
                sa.Integer(),
                sa.Computed(AMOUNT_CENTS_SQL, persisted=True),
                nullable=False,
            )
        )
        batch_op.add_column(
            sa.Column(
                "total_cents",
                sa.Integer(),
                sa.Computed(TOTAL_CENTS_SQL, persisted=True),
                nullable=False,
            )
        )
        batch_op.create_unique_constraint(
            "uq_settlement_detail_supplier_order",
            ["supplier_code", "order_no"],
        )

    _rebuild_receipt_line_with_fk()

    op.execute(sa.text(PLAN.drop_guard_statement()))


def downgrade():
    # 只还原表结构；已合并的明细与改挂的收货行不可逆。
    _rebuild_receipt_line_without_fk()

    new_settlement_detail = sa.Table(
        "settlement_detail",
        sa.MetaData(),
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("supplier_code", sa.Text(), nullable=False),
        sa.Column("order_no", sa.Text(), nullable=False),
        sa.Column("amount_text", sa.Text(), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column(
            "amount_cents",
            sa.Integer(),
            sa.Computed(AMOUNT_CENTS_SQL, persisted=True),
            nullable=False,
        ),
        sa.Column(
            "total_cents",
            sa.Integer(),
            sa.Computed(TOTAL_CENTS_SQL, persisted=True),
            nullable=False,
        ),
        sa.UniqueConstraint(
            "supplier_code", "order_no", name="uq_settlement_detail_supplier_order"
        ),
    )
    with op.batch_alter_table(
        "settlement_detail", copy_from=new_settlement_detail, recreate="always"
    ) as batch_op:
        batch_op.drop_constraint(
            "uq_settlement_detail_supplier_order", type_="unique"
        )
        batch_op.drop_column("total_cents")
        batch_op.drop_column("amount_cents")
        batch_op.alter_column(
            "quantity",
            new_column_name="quantity_text",
            type_=sa.Text(),
            existing_type=sa.Integer(),
        )

    _rebuild_receipt_line_with_fk()
