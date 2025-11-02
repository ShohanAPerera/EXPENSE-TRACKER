from flask import Flask, render_template, request, url_for, make_response, flash, redirect
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime, date
from sqlalchemy import func
import subprocess
import os


import oracledb


app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///expenses.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SECRET_KEY'] = 'my-secret-key'


db = SQLAlchemy(app)

class Expense(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    description = db.Column(db.String(200), nullable=False)
    amount = db.Column(db.Float, nullable=False)
    category = db.Column(db.String(100), nullable=False)
    date = db.Column(db.DateTime, nullable=False)

class CategoryBudget(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False, unique=True)
    budget_amount = db.Column(db.Float, nullable=False)
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class Saving(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    description = db.Column(db.String(200), nullable=False)
    amount = db.Column(db.Float, nullable=False)
    date = db.Column(db.DateTime, nullable=False)
    type = db.Column(db.String(50), nullable=False)  # 'deposit' or 'withdrawal'
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


# Default categories
DEFAULT_CATEGORIES = ["Food", "Transport", "Utilities", "Entertainment", "Other"]

def init_db():
    """Initialize database with default categories"""
    with app.app_context():
        db.create_all()
        for category_name in DEFAULT_CATEGORIES:
            if not CategoryBudget.query.filter_by(name=category_name).first():
                category = CategoryBudget(name=category_name, budget_amount=1000.00)
                db.session.add(category)
        db.session.commit()

# Initialize database
init_db()

def parse_date_or_none(s: str):
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d") 
    except ValueError:
        return None

def get_category_stats(start_date=None, end_date=None):
    """Get spending statistics for all categories"""
    categories = CategoryBudget.query.filter_by(is_active=True).all()
    category_stats = {}
    
    for category in categories:
        # Calculate total expenses for this category
        expense_q = Expense.query.filter(Expense.category == category.name)
        if start_date:
            expense_q = expense_q.filter(Expense.date >= start_date)
        if end_date:
            expense_q = expense_q.filter(Expense.date <= end_date)
        
        total_expenses = expense_q.with_entities(func.sum(Expense.amount)).scalar() or 0
        remaining = category.budget_amount - total_expenses
        percentage_used = (total_expenses / category.budget_amount * 100) if category.budget_amount > 0 else 0
        
        category_stats[category.name] = {
            'budget': category,
            'total_expenses': round(total_expenses, 2),
            'remaining': round(remaining, 2),
            'percentage_used': round(percentage_used, 2),
            'over_budget': remaining < 0,
            'is_active': remaining > 0 and category.is_active  # Hide from dropdown if budget exhausted
        }
    
    return category_stats

@app.route("/")
def index():
    # Safely get query parameters
    start_str = (request.args.get("start") or "").strip()
    end_str = (request.args.get("end") or "").strip()
    selected_category = (request.args.get("category") or "").strip()

    # Parse dates
    start_date = parse_date_or_none(start_str)
    end_date = parse_date_or_none(end_str)

    # Start building query
    q = Expense.query

    # Validate date range
    if start_date and end_date and end_date < start_date:
        flash("End date cannot be earlier than start date.", "error")
        # Reset invalid dates
        start_date = end_date = None
        start_str = end_str = ""

    # Apply filters
    if start_date:
        q = q.filter(Expense.date >= start_date)
    if end_date:
        q = q.filter(Expense.date <= end_date)
    if selected_category:
        q = q.filter(Expense.category == selected_category)

    # Fetch data
    expenses = q.order_by(Expense.date.desc(), Expense.id.desc()).all()
    total = round(sum(e.amount for e in expenses), 2)

    # Get category statistics
    category_stats = get_category_stats(start_date, end_date)
    
    # Get active categories for dropdown (where budget is not exhausted)
    active_categories = [cat for cat, stats in category_stats.items() if stats['is_active']]

    # Get savings data
    savings = Saving.query.order_by(Saving.date.desc(), Saving.id.desc()).all()
    
    # Calculate savings total
    savings_total = 0
    for saving in savings:
        if saving.type == 'deposit':
            savings_total += saving.amount
        else:  # withdrawal
            savings_total -= saving.amount

    # Charts data
    cat_q = db.session.query(Expense.category, func.sum(Expense.amount))
    if start_date:
        cat_q = cat_q.filter(Expense.date >= start_date)
    if end_date:
        cat_q = cat_q.filter(Expense.date <= end_date)
    if selected_category:
        cat_q = cat_q.filter(Expense.category == selected_category)

    # Day chart
    day_q = db.session.query(Expense.date, func.sum(Expense.amount))
    if start_date:
        day_q = day_q.filter(Expense.date >= start_date)
    if end_date:
        day_q = day_q.filter(Expense.date <= end_date)
    if selected_category:
        day_q = day_q.filter(Expense.category == selected_category)

    day_rows = day_q.group_by(Expense.date).order_by(Expense.date).all() 
    day_labels = [d.strftime("%b %d") for d, _ in day_rows]  
    day_values = [round(float(s or 0), 2) for _, s in day_rows]

    cat_rows = cat_q.group_by(Expense.category).all() 
    cat_labels = [c for c, _ in cat_rows]
    cat_values = [round(float(s or 0), 2) for _, s in cat_rows]

    # Check if categories have expenses for the delete confirmation message
    categories_with_expenses = {}
    all_categories = CategoryBudget.query.all()
    for category in all_categories:
        has_expenses = Expense.query.filter_by(category=category.name).first() is not None
        categories_with_expenses[category.id] = has_expenses

    # Render page
    return render_template(
        "index.html",
        categories=active_categories,
        all_categories=all_categories,
        category_stats=category_stats,
        categories_with_expenses=categories_with_expenses,
        today_str=date.today().isoformat(),
        expenses=expenses,
        savings=savings,
        savings_total=savings_total,
        total=total,
        start_str=start_str,
        end_str=end_str,
        cat_labels=cat_labels,
        cat_values=cat_values,
        selected_category=selected_category,
        day_labels=day_labels,
        day_values=day_values
    )


@app.route("/add-saving", methods=["POST"])
def add_saving():
    description = (request.form.get("description") or "").strip()
    amount_str = request.form.get("amount") or "0"
    date_str = (request.form.get("date") or "").strip()
    saving_type = request.form.get("type") or "deposit"

    # Validate required fields
    if not description or not amount_str:
        flash("Description and amount are required!", "error")
        return redirect(url_for("index"))
    
    # Validate and convert amount
    try:
        amount = float(amount_str)
        if amount <= 0:
            flash("Amount must be positive.", "error")
            return redirect(url_for("index"))
    except ValueError:
        flash("Invalid amount. Please enter a positive number.", "error")
        return redirect(url_for("index"))
    
    # Handle date
    try:
        if date_str:
            saving_date = datetime.strptime(date_str, "%Y-%m-%d")
        else:
            saving_date = datetime.today()
    except ValueError:
        flash("Invalid date format. Using today's date.", "warning")
        saving_date = datetime.today()
    
    # Create and save saving
    try:
        saving = Saving(
            description=description, 
            amount=amount, 
            date=saving_date,
            type=saving_type
        )
        db.session.add(saving)
        db.session.commit()
        flash(f"Savings {saving_type} added successfully", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Error saving savings record: {str(e)}", "error")
    
    return redirect(url_for("index"))

@app.route("/delete-saving/<int:saving_id>", methods=["POST"])
def delete_saving(saving_id):
    saving = Saving.query.get_or_404(saving_id)
    try:
        db.session.delete(saving)
        db.session.commit()
        flash("Savings record deleted successfully", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Error deleting savings record: {str(e)}", "error")
    return redirect(url_for("index"))


@app.route("/add-category", methods=["POST"])
def add_category():
    name = (request.form.get("name") or "").strip()
    budget_amount_str = request.form.get("budget_amount") or "0"
    
    if not name:
        flash("Category name is required!", "error")
        return redirect(url_for("index"))
    
    try:
        budget_amount = float(budget_amount_str)
        if budget_amount <= 0:
            flash("Budget amount must be positive.", "error")
            return redirect(url_for("index"))
    except ValueError:
        flash("Invalid budget amount.", "error")
        return redirect(url_for("index"))
    
    # Check if category already exists
    existing_category = CategoryBudget.query.filter_by(name=name).first()
    if existing_category:
        # Reactivate if exists but inactive
        if not existing_category.is_active:
            existing_category.is_active = True
            existing_category.budget_amount = budget_amount
            db.session.commit()
            flash(f"Category '{name}' reactivated with budget ${budget_amount:.2f}", "success")
        else:
            flash(f"Category '{name}' already exists!", "error")
        return redirect(url_for("index"))
    
    try:
        category = CategoryBudget(name=name, budget_amount=budget_amount)
        db.session.add(category)
        db.session.commit()
        flash(f"Category '{name}' added successfully with budget ${budget_amount:.2f}", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Error adding category: {str(e)}", "error")
    
    return redirect(url_for("index"))

@app.route("/edit-category/<int:category_id>", methods=["POST"])
def edit_category(category_id):
    category = CategoryBudget.query.get_or_404(category_id)
    budget_amount_str = request.form.get("budget_amount") or "0"
    
    try:
        budget_amount = float(budget_amount_str)
        if budget_amount <= 0:
            flash("Budget amount must be positive.", "error")
            return redirect(url_for("index"))
    except ValueError:
        flash("Invalid budget amount.", "error")
        return redirect(url_for("index"))
    
    try:
        category.budget_amount = budget_amount
        db.session.commit()
        flash(f"Category '{category.name}' budget updated to ${budget_amount:.2f}", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Error updating category: {str(e)}", "error")
    
    return redirect(url_for("index"))

@app.route("/delete-category/<int:category_id>", methods=["POST"])
def delete_category(category_id):
    category = CategoryBudget.query.get_or_404(category_id)
    category_name = category.name
    
    # Check if category has expenses
    has_expenses = Expense.query.filter_by(category=category_name).first() is not None
    
    try:
        if has_expenses:
            # Instead of deleting, deactivate the category
            category.is_active = False
            flash(f"Category '{category_name}' deactivated (has existing expenses)", "warning")
        else:
            db.session.delete(category)
            flash(f"Category '{category_name}' deleted successfully", "success")
        
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        flash(f"Error deleting category: {str(e)}", "error")
    
    return redirect(url_for("index"))

@app.route("/add", methods=["POST"])
def add():
    description = (request.form.get("description") or "").strip()
    amount_str = request.form.get("amount") or "0"
    category = (request.form.get("category") or "").strip()
    date_str = (request.form.get("date") or "").strip()

    # Validate required fields
    if not description or not amount_str or not category:
        flash("Description, amount, and category are required!", "error")
        return redirect(url_for("index"))
    
    # Validate and convert amount
    try:
        amount = float(amount_str)
        if amount <= 0:
            flash("Amount must be positive.", "error")
            return redirect(url_for("index"))
    except ValueError:
        flash("Invalid amount. Please enter a positive number.", "error")
        return redirect(url_for("index"))
    
    # Handle date
    try:
        if date_str:
            expense_date = datetime.strptime(date_str, "%Y-%m-%d")
        else:
            expense_date = datetime.today()
    except ValueError:
        flash("Invalid date format. Using today's date.", "warning")
        expense_date = datetime.today()
    
    # Create and save expense
    try:
        e = Expense(
            description=description, 
            amount=amount, 
            category=category, 
            date=expense_date
        )
        db.session.add(e)
        db.session.commit()
        flash("Expense added successfully", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Error saving expense: {str(e)}", "error")
    
    return redirect(url_for("index"))

@app.route("/delete/<int:expense_id>", methods=["POST"])
def delete(expense_id):
    expense = Expense.query.get_or_404(expense_id)
    try:
        db.session.delete(expense)
        db.session.commit()
        flash("Expense deleted successfully", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Error deleting expense: {str(e)}", "error")
    return redirect(url_for("index"))



@app.route("/sync-to-oracle", methods=["POST"])
def sync_to_oracle():
    """Call sync function directly instead of using subprocess"""
    print("🔄 SYNC BUTTON CLICKED - Starting direct sync...")
    
    try:
        # Import and call the sync function directly
        import sync_to_oracle
        
        result = sync_to_oracle.sync_data()
        print(f"✅ Sync result: {result}")
        
        if "❌" in result or "error" in result.lower() or "failed" in result.lower():
            flash(f"❌ {result}", "error")
        else:
            flash(f"✅ {result}", "success")
            
    except ImportError as e:
        error_msg = f"Could not import sync module: {str(e)}"
        print(f"❌ {error_msg}")
        flash(f"❌ {error_msg}", "error")
    except Exception as e:
        error_msg = f"Sync error: {str(e)}"
        print(f"❌ {error_msg}")
        flash(f"❌ {error_msg}", "error")
    
    return redirect(url_for("index"))

if __name__ == "__main__":
    app.run(debug=True, port=4848)