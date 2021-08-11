from lsrs import app
from lsrs import db
from flask import jsonify
import json
from flask import render_template, request, redirect, url_for
from datetime import datetime


@app.route('/')
@app.route('/home')
def home_page():
    sql = """SELECT 'Product' AS name, count(pid) AS count FROM Product
            UNION ALL
            SELECT 'Store' AS name, count(store_number) FROM Store
            UNION ALL
            SELECT 'Store with food' AS name, count(store_number) FROM Store 
            WHERE restaurant = TRUE OR snack_bar = TRUE
            UNION ALL
            SELECT 'Store with childcare' AS name, count(time_limit) FROM Store WHERE time_limit <> 0
            UNION ALL
            SELECT 'Campaign' AS name, count(description) FROM AdCamp;"""
    results = db.connectToMySQL().query_db(sql)
    return render_template('home.html', reports=results)


@app.route('/category_report')
def category_report():
    sql = "SELECT c.category_name, COUNT(*) AS total_quantity, MAX(p.retail_price) AS max_retail_price, MIN(p.retail_price) AS min_retail_price, AVG(p.retail_price) AS average_retail_price " \
          "FROM Product AS p JOIN AssignTo AS a ON p.pid = a.pid JOIN Category AS c ON c.category_name = a.category_name GROUP BY c.category_name ORDER BY c.category_name ASC";
    results = db.connectToMySQL().query_db(sql)
    return render_template('category_report.html', reports=results)


@app.route('/couches_sofa_actual_predicted_revenue_report')
def couches_sofa_actual_predicted_revenue_report():
    category = request.args['category']
    sql = """
    SELECT pid, product_name, retail_price, total_sale_number, discount_sale_number, retail_sale_number, 
	actual_revenue, predicted_revenue, IF(ABS(actual_revenue - predicted_revenue) > 5000, actual_revenue - predicted_revenue, '') 
	AS difference FROM
	(SELECT all_prd.pid, all_prd.product_name, all_prd.retail_price, dis_prd_sale.total_sale_number, dis_prd_sale.discount_sale_number, 
		(dis_prd_sale.total_sale_number - dis_prd_sale.discount_sale_number) AS retail_sale_number, 
		((dis_prd_sale.total_sale_number - dis_prd_sale.discount_sale_number) * all_prd.retail_price + dis_prd_sale.discount_sale) AS actual_revenue,
		(dis_prd_sale.discount_sale_number * 0.75 + dis_prd_sale.total_sale_number - dis_prd_sale.discount_sale_number) * all_prd.retail_price AS predicted_revenue
	FROM (SELECT p.pid, p.product_name, p.retail_price FROM Product AS p JOIN AssignTo AS a ON p.pid = a.pid JOIN Category AS c ON c.category_name = a.category_name 
	WHERE c.category_name = '%s') AS all_prd JOIN (SELECT s.pid, SUM(s.quantity) AS total_sale_number, SUM(if(prd_dis.dis_price > 0, s.quantity, 0)) AS discount_sale_number,
	SUM(IF(prd_dis.dis_price > 0, prd_dis.dis_price, 0) * s.quantity) AS discount_sale FROM Sale AS s LEFT JOIN 
		(SELECT hd.time_stamp, prd.pid, prd.product_name, prd.retail_price, hd.dis_price FROM 
		(SELECT p.pid, p.product_name, p.retail_price FROM Product AS p JOIN AssignTo AS a ON p.pid = a.pid JOIN Category AS c ON c.category_name = a.category_name 
		    WHERE c.category_name = '%s') AS prd
		    JOIN HaveDiscount AS hd ON prd.pid = hd.pid JOIN SaleDate AS sd ON hd.time_stamp = sd.time_stamp) AS prd_dis
		ON s.time_stamp = prd_dis.time_stamp AND s.pid = prd_dis.pid
		GROUP BY s.pid) as dis_prd_sale ON all_prd.pid = dis_prd_sale.pid) AS rst_tbl ORDER BY actual_revenue - predicted_revenue DESC;
    """ % (category, category)
    results = db.connectToMySQL().query_db(sql)
    return render_template('couches_sofa_actual_predicted_revenue_report.html', reports=results)


@app.route('/store_revenue_page')
def store_revenue_page():
    sql = "SELECT DISTINCT state FROM City"
    results = db.connectToMySQL().query_db(sql)
    return render_template('store_revenue_page.html', states=results)


@app.route('/store_revenue_data', methods=['GET', 'POST'])
def store_revenue_data():
    state = request.args.get("state")
    sql = """SELECT st.store_number, st.street_address, st.city_name, sales_store.sales_year, sales_store.sales AS revenue 
        FROM Store AS st JOIN (SELECT YEAR(s.time_stamp) AS sales_year, s.store_number, SUM(if(t.dis_price > 0, t.dis_price, p.retail_price) * s.quantity) AS sales
	    FROM Sale AS s JOIN Product p ON s.pid = p.pid LEFT JOIN (SELECT hd.time_stamp, hd.pid, hd.dis_price FROM Product AS p JOIN HaveDiscount AS hd ON p.pid = hd.pid 
	    JOIN SaleDate AS sd ON sd.time_stamp = hd.time_stamp) AS t ON s.time_stamp = t.time_stamp AND s.pid = t.pid
        GROUP BY YEAR(s.time_stamp), s.store_number) AS sales_store ON st.store_number = sales_store.store_number 
        WHERE st.state = '%s' ORDER BY sales_store.sales_year ASC, sales_store.sales DESC
    """ % state
    results = db.connectToMySQL().query_db(sql)
    return jsonify(results)


@app.route('/store_revenue_report')
def store_revenue_report():
    results = request.args['reports']
    state = request.args['state']
    return render_template('store_revenue_report.html', reports=json.loads(results), state=state)
    
# @app.route('/store_revenue_report')
# def store_revenue_report():
#     results = request.args['reports']
#     state = request.args['state']
#     return render_template('store_revenue_report.html', reports=json.loads(results), state=state)

# Total revenue by population report
@app.route('/revenue_by_population/startyear=<sy>&endyear=<ey>')
def revenue_by_population(sy, ey):
    sql = """
    SELECT AA.year, AA.revenue AS revenue_1, BB.revenue AS revenue_2, CC.revenue AS revenue_3, DD.revenue AS revenue_4
    FROM
    ((SELECT YEAR(Sale.time_stamp) AS "year", SUM(COALESCE(dis_price, retail_price) * quantity) AS revenue
    FROM Store JOIN
    (SELECT city_name, state, population
    FROM City) A ON (Store.city_name=A.city_name AND Store.state=A.state)
    JOIN Sale ON (Store.store_number=Sale.store_number)
    JOIN Product ON (Sale.pid=Product.pid)
    LEFT JOIN HaveDiscount ON (Sale.time_stamp=HaveDiscount.time_stamp AND Sale.pid=HaveDiscount.pid)

    WHERE (YEAR(Sale.time_stamp) BETWEEN %s AND %s) AND (population < 3700000)
    GROUP BY YEAR(Sale.time_stamp)
    ORDER BY year) AA
    JOIN
    (SELECT YEAR(Sale.time_stamp) AS "year", SUM(COALESCE(dis_price, retail_price) * quantity) AS revenue
    FROM Store JOIN
    (SELECT city_name, state, population
    FROM City) A ON (Store.city_name=A.city_name AND Store.state=A.state)
    JOIN Sale ON (Store.store_number=Sale.store_number)
    JOIN Product ON (Sale.pid=Product.pid)
    LEFT JOIN HaveDiscount ON (Sale.time_stamp=HaveDiscount.time_stamp AND Sale.pid=HaveDiscount.pid)

    WHERE (YEAR(Sale.time_stamp) BETWEEN %s AND %s) AND (population < 6700000 AND population >= 3700000)
    GROUP BY YEAR(Sale.time_stamp)
    ORDER BY year) BB ON AA.year = BB.year
    JOIN
    (SELECT YEAR(Sale.time_stamp) AS "year", SUM(COALESCE(dis_price, retail_price) * quantity) AS revenue
    FROM Store JOIN
    (SELECT city_name, state, population
    FROM City) A ON (Store.city_name=A.city_name AND Store.state=A.state)
    JOIN Sale ON (Store.store_number=Sale.store_number)
    JOIN Product ON (Sale.pid=Product.pid)
    LEFT JOIN HaveDiscount ON (Sale.time_stamp=HaveDiscount.time_stamp AND Sale.pid=HaveDiscount.pid)

    WHERE (YEAR(Sale.time_stamp) BETWEEN %s AND %s) AND (population < 9000000 AND population >= 6700000)
    GROUP BY YEAR(Sale.time_stamp)
    ORDER BY year) CC ON AA.year = CC.year
    JOIN
    (SELECT YEAR(Sale.time_stamp) AS "year", SUM(COALESCE(dis_price, retail_price) * quantity) AS revenue
    FROM Store JOIN
    (SELECT city_name, state, population
    FROM City) A ON (Store.city_name=A.city_name AND Store.state=A.state)
    JOIN Sale ON (Store.store_number=Sale.store_number)
    JOIN Product ON (Sale.pid=Product.pid)
    LEFT JOIN HaveDiscount ON (Sale.time_stamp=HaveDiscount.time_stamp AND Sale.pid=HaveDiscount.pid)

    WHERE (YEAR(Sale.time_stamp) BETWEEN %s AND %s) AND (population >= 9000000)
    GROUP BY YEAR(Sale.time_stamp)
    ORDER BY year) DD ON AA.year = DD.year)
    ORDER BY AA.year;
    """ % (sy, ey, sy, ey, sy, ey, sy, ey)
    results = db.connectToMySQL().query_db(sql)
    return render_template('revenue_by_population.html', reports=results)



@app.route('/revenue_by_population_years', methods = ['POST', 'GET'])
def revenue_by_population_years():
    if request.method == 'POST':
        sy = request.form['sy']
        ey = request.form['ey']
        if not sy or not ey:
            return render_template('revenue_by_population_years.html', error_message="Input years are empty")
        try:
            int(sy)
            int(ey)
        except Exception as e:
            print("Something went wrong", e)
            return render_template('revenue_by_population_years.html', error_message="Inputs are invalid")
        if int(sy) > int(ey):
            print("End year is larger than start year!!!")
            return render_template('revenue_by_population_years.html', error_message="Start year is larger than end year!!!")
        return redirect(url_for('revenue_by_population', sy=sy, ey=ey))
    else:
        sy = request.args.get('sy')
        ey = request.args.get('ey')
        return render_template('revenue_by_population_years.html')



# Total sales by Childcare report
@app.route('/total_sales_by_childcare')
def total_sales_by_childcare(): #
    sql = """SELECT AA.month, revenue_1, revenue_2, revenue_3, revenue_4
    FROM
    (SELECT DATE_FORMAT(Sale.time_stamp,'%Y-%m') AS month,
                    SUM(COALESCE(dis_price, retail_price) * quantity) AS revenue_1

                    FROM Store JOIN Sale ON (Store.store_number=Sale.store_number)
                    JOIN Product ON (Sale.pid=Product.pid)
                    LEFT JOIN HaveDiscount ON (HaveDiscount.pid=Sale.pid AND HaveDiscount.time_stamp=Sale.time_stamp)
                    JOIN (SELECT MAX(time_stamp) AS curr_date FROM SaleDate) a

                    WHERE (12*(YEAR(a.curr_date)-YEAR(Sale.time_stamp))+MONTH(a.curr_date)-MONTH(Sale.time_stamp) < 12) AND (time_limit=0)

                    GROUP BY DATE_FORMAT(Sale.time_stamp,'%Y-%m'), time_limit
                    ORDER BY month, time_limit) AA
                    JOIN
                    (SELECT DATE_FORMAT(Sale.time_stamp,'%Y-%m') AS month,
                    SUM(COALESCE(dis_price, retail_price) * quantity) AS revenue_2

                    FROM Store JOIN Sale ON (Store.store_number=Sale.store_number)
                    JOIN Product ON (Sale.pid=Product.pid)
                    LEFT JOIN HaveDiscount ON (HaveDiscount.pid=Sale.pid AND HaveDiscount.time_stamp=Sale.time_stamp)
                    JOIN (SELECT MAX(time_stamp) AS curr_date FROM SaleDate) a

                    WHERE (12*(YEAR(a.curr_date)-YEAR(Sale.time_stamp))+MONTH(a.curr_date)-MONTH(Sale.time_stamp) < 12) AND (time_limit=30)

                    GROUP BY DATE_FORMAT(Sale.time_stamp,'%Y-%m'), time_limit
                    ORDER BY month, time_limit) BB ON AA.month = BB.month
                    JOIN
                    (SELECT DATE_FORMAT(Sale.time_stamp,'%Y-%m') AS month,
                    SUM(COALESCE(dis_price, retail_price) * quantity) AS revenue_3

                    FROM Store JOIN Sale ON (Store.store_number=Sale.store_number)
                    JOIN Product ON (Sale.pid=Product.pid)
                    LEFT JOIN HaveDiscount ON (HaveDiscount.pid=Sale.pid AND HaveDiscount.time_stamp=Sale.time_stamp)
                    JOIN (SELECT MAX(time_stamp) AS curr_date FROM SaleDate) a

                    WHERE (12*(YEAR(a.curr_date)-YEAR(Sale.time_stamp))+MONTH(a.curr_date)-MONTH(Sale.time_stamp) < 12) AND (time_limit=45)

                    GROUP BY DATE_FORMAT(Sale.time_stamp,'%Y-%m'), time_limit
                    ORDER BY month, time_limit) CC ON AA.month = CC.month
                    JOIN
                    (SELECT DATE_FORMAT(Sale.time_stamp,'%Y-%m') AS month,
                    SUM(COALESCE(dis_price, retail_price) * quantity) AS revenue_4

                    FROM Store JOIN Sale ON (Store.store_number=Sale.store_number)
                    JOIN Product ON (Sale.pid=Product.pid)
                    LEFT JOIN HaveDiscount ON (HaveDiscount.pid=Sale.pid AND HaveDiscount.time_stamp=Sale.time_stamp)
                    JOIN (SELECT MAX(time_stamp) AS curr_date FROM SaleDate) a

                    WHERE (12*(YEAR(a.curr_date)-YEAR(Sale.time_stamp))+MONTH(a.curr_date)-MONTH(Sale.time_stamp) < 12) AND (time_limit=60)

                    GROUP BY DATE_FORMAT(Sale.time_stamp,'%Y-%m'), time_limit
                    ORDER BY month, time_limit) DD ON AA.month = DD.month
                    ORDER BY AA.month;"""
    results = db.connectToMySQL().query_db(sql)
    return render_template('total_sales_by_childcare.html', reports=results)




@app.route('/restaurant_impact_sale')
def restaurant_impact_sale():
    sql = """
    SELECT AssignTo.category_name AS Category, 
       CASE 
           WHEN Store.restaurant=0 THEN "Non-restaurant"
           ELSE "Restaurant"
       END AS 'Store_Type',
       SUM(Sale.quantity) AS Quantity_Sold
    FROM AssignTo
    JOIN  Sale ON AssignTo.PID=Sale.PID
    JOIN Store ON Store.store_number=Sale.store_number
    GROUP BY AssignTo.category_name, Store.restaurant
    ORDER BY AssignTo.category_name ASC, Store.restaurant ASC;
    """
    results = db.connectToMySQL().query_db(sql)
    return render_template('restraunt_impact_sale.html', reports=results)



@app.route('/compaign_impact_sale')
def compaign_impact_sale():
    sql = """
    SELECT res.PID AS 'Product_ID',
       res.product_name AS 'Product_Name', 
       res.ds AS 'Sold_During_Campaign',
       res.os AS 'Sold_Outside_Campaign',
       res.diff AS 'Difference'
    FROM (
        (SELECT total.PID, 
            total.product_name, 
            IFNULL(during.during_sale, 0) AS ds, 
            (total.total_sale-IFNULL(during.during_sale, 0)) AS os, 
            (IFNULL(during.during_sale, 0)-(total.total_sale-IFNULL(during.during_sale, 0))) AS diff
        FROM (
            SELECT p.PID, 
                p.product_name, 
                SUM(s.quantity) AS total_sale
            FROM Product AS p
            JOIN HaveDiscount AS d ON p.PID=d.PID
            JOIN Sale AS s ON d.PId=s.PID AND d.time_stamp=s.time_stamp
            GROUP BY d.PID
        ) total 
        LEFT JOIN (
            SELECT p.PID, 
				p.product_name, 
				SUM(s.quantity) AS during_sale
			FROM Product AS p
			JOIN HaveDiscount AS d ON p.PID=d.PID
			JOIN Sale AS s ON d.PId=s.PID AND d.time_stamp=s.time_stamp
			JOIN (
			select distinct e.time_stamp
			From HaveCamp AS c
			JOIN SaleDate AS e ON e.time_stamp=c.time_stamp
			)t
			ON t.time_stamp=s.time_stamp
			GROUP BY d.PID
        ) during 
        ON total.PID=during.PID 
        ORDER BY diff DESC
        LIMIT 10)

        UNION 

        (SELECT total.PID, 
            total.product_name, 
            IFNULL(during.during_sale, 0) AS ds, 
            (total.total_sale-IFNULL(during.during_sale, 0)) AS os, 
            (IFNULL(during.during_sale, 0)-(total.total_sale-IFNULL(during.during_sale, 0))) AS diff
        FROM (
            SELECT p.PID, 
                p.product_name, 
                SUM(s.quantity) AS total_sale
            FROM Product AS p
            JOIN HaveDiscount AS d ON p.PID=d.PID
            JOIN Sale AS s ON d.PId=s.PID AND d.time_stamp=s.time_stamp
            GROUP BY d.PID
        ) total 
        LEFT JOIN (
            SELECT p.PID, 
				p.product_name, 
				SUM(s.quantity) AS during_sale
			FROM Product AS p
			JOIN HaveDiscount AS d ON p.PID=d.PID
			JOIN Sale AS s ON d.PId=s.PID AND d.time_stamp=s.time_stamp
			JOIN (
			select distinct e.time_stamp
			From HaveCamp AS c
			JOIN SaleDate AS e ON e.time_stamp=c.time_stamp
			)t
			ON t.time_stamp=s.time_stamp
			GROUP BY d.PID
        ) during 
        ON total.PID=during.PID
        ORDER BY diff ASC
        LIMIT 10)
    ) res
    ORDER BY res.diff DESC;
    """
    results = db.connectToMySQL().query_db(sql)
    return render_template('campaign_impact_sale.html', reports=results)


@app.route('/outdoor_furniture_sale')
def outdoor_furniture_sale():
    sql = """
    SELECT year, total_sale, daily_sale, groundhog_sale FROM  
    (SELECT (YEAR(time_stamp)) AS year, SUM(quantity) AS total_sale, (SUM(quantity)/365) AS daily_sale FROM Sale JOIN (SELECT pid FROM AssignTo WHERE category_name = "Outdoor Furniture") AS outdoor USING(pid) GROUP BY YEAR(time_stamp)) AS avesale 
    JOIN 
    (SELECT (YEAR(time_stamp)) AS year, (SUM(quantity)) AS groundhog_sale FROM Sale JOIN (SELECT pid FROM AssignTo WHERE category_name = "Outdoor Furniture") AS outdoor USING(pid) WHERE MONTH(time_stamp) = 2 AND DAY(time_stamp) = 2 GROUP BY YEAR(time_stamp)) AS realsale 
    USING (year)
    ORDER BY year ASC;
    """

    results = db.connectToMySQL().query_db(sql)
    return render_template('outdoor_furniture_sale.html', reports=results)

@app.route('/view_holiday')
def view_holiday():
    sql = """
    SELECT time_stamp, holiday_name FROM Holiday;
    """

    results = db.connectToMySQL().query_db(sql)
    return render_template('view_holiday.html', reports=results)

@app.route('/view_added_holiday/<holiday>')
def view_added_holiday(holiday):
    sql = ("""
    SELECT time_stamp, holiday_name FROM Holiday WHERE time_stamp=%s;
    """)

    results = db.connectToMySQL().query_db(sql, (holiday))
    if not results:
        return render_template('add_holiday.html', error_message="Add failed, didn't found result")
    return render_template('view_holiday.html', reports=results)

@app.route('/add_holiday', methods = ['POST', 'GET'])
def add_holiday():
    if request.method == 'POST':
        holiday_date = request.form['date']
        holiday_name = request.form['name']
        if not holiday_date or not holiday_name:
            return render_template('add_holiday.html', error_message="Holiday Date or name empty")
        if len(holiday_name) > 50:
            return render_template('add_holiday.html', error_message="Holiday name too long")
        try:
            datetime.strptime(holiday_date, '%Y-%m-%d')
        except Exception as e:
            print("Something went wrong", e)
            return render_template('add_holiday.html', error_message="Holiday Date format is invalid")
        sql = """
        INSERT INTO Holiday (time_stamp, holiday_name) VALUES(%s, %s);
        """
        results = db.connectToMySQL().query_db(sql, (holiday_date, holiday_name))
        print("!!!!!!!!", results)
        if results is False:
            return render_template('add_holiday.html', error_message="Holiday Date already exsisted")
        return redirect(url_for('view_added_holiday', holiday = holiday_date))
    else:
        holiday_date = request.args.get('holiday_date')
        return render_template('add_holiday.html')

@app.route('/view_city_population')
def view_city_population():
    sql = """
    SELECT population, city_name, state FROM City;
    """

    results = db.connectToMySQL().query_db(sql)
    return render_template('view_city_population.html', reports=results)

@app.route('/edit_city_population_city', methods = ['POST', 'GET'])
def edit_city_population_city():
    if request.method == 'POST':
        city = request.form['City']
        state = request.form['State']
        population = request.form['Population']
        print(type(population))
        if not city or not state or not population:
            return render_template('edit_city_population_city.html', error_message="Input city/state are empty")
        if population.isdigit() is False:
            return render_template('edit_city_population_city.html', error_message="population is not an integer")
        return redirect(url_for('edit_city_population', city=city, state=state, population=population))
    else:
        city = request.args.get('city')
        state = request.args.get('state')
        population = request.args.get('Population')
        return render_template('edit_city_population_city.html')

@app.route('/edit_city_population/c=<city>&s=<state>&p=<population>')
def edit_city_population(city, state, population):
    sql = """
          SELECT population FROM City WHERE city_name = %s and state = %s;
             """
    results = db.connectToMySQL().query_db(sql, (city, state))
    print("!!!!!!!!", results)
    print(type(results))
    print("????", sql)
    if len(results) == 0:
        return render_template('edit_city_population_city.html', error_message="City/State is not exsisted")
    elif len(population) > 9:
        return render_template('edit_city_population_city.html', error_message="Please edit valid city population")
    else:
        sql = """
            UPDATE City SET population = %s WHERE city_name = %s and state = %s;
            """
        db.connectToMySQL().query_db(sql, (population, city, state))
    return render_template('edit_city_population.html', state= state, city=city, population=population)


@app.route('/storeSale_info_each_category_month', methods = ['POST', 'GET'])
def storeSale_info_each_category_month():
    if request.method == 'POST':
        year = request.form['year']
        month = request.form['month']
        if not year or not month:
            return render_template('storeSale_info_each_category_month.html', error_message="Input years are empty")
        if year.isdigit() is False or month.isdigit() is False:
                    return render_template('storeSale_info_each_category_month.html', error_message="year/month is not an integer")

        return redirect(url_for('storeSale_info_each_category', year=year, month=month))
    else:
        year = request.args.get('year')
        month = request.args.get('month')
        return render_template('storeSale_info_each_category_month.html')

@app.route('/storeSale_info_each_category/y=<year>&m=<month>')
def storeSale_info_each_category(year, month):
    sql = """SELECT category_name, state, numberOfUnit
             FROM (
             SELECT category_name, state, numberOfUnit, rank() over (partition by category_name ORDER BY numberOfUnit desc) as rn
             FROM (
             SELECT category_name, state, SUM(quantity) as numberOfUnit FROM Store JOIN Sale ON (Store.store_number=Sale.store_number)
             JOIN AssignTo ON (Sale.pid=AssignTo.pid) where YEAR(time_stamp) = %s and MONTH(time_stamp) = %s GROUP BY category_name, state) a
             )b
             WHERE rn = 1 ORDER BY category_name ASC;""" % (year, month)
    results = db.connectToMySQL().query_db(sql)
    return render_template('storeSale_info_each_category.html', reports=results)