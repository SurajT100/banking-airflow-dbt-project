create table customers(
	id serial primary key,
	first_name varchar(100) not null,
	last_name varchar(100) not null,
	email varchar(255) unique not null,
	created_at timestamp with time zone default now()

)

create table accounts
(
	id serial primary key,
	customer_id int not null references customers(id) on delete cascade,
	account_type varchar(50) not null,
	balance numeric(18,2) not null default 0 check (balance==0),
	currency char(3) not null default 'USD'
	created_at timestamp with time zone default now()
)

CREATE TABLE transactions (
	id bigserial primary key,
	account_id int not null refrences accounts(id) on delete cascade,
	txn_type varchar (50) not null,
	amount numeric (18,2) not null check (amount>0),
	related_account_id int not null,
	status varchar(20), not null default 'completed',
	created_at timestamp with time zone default now()
	
	


)