/**
 * 備品管理システム バックエンドAPI (Google Apps Script)
 * FastAPI + SQLAlchemy の構造を GAS + Spreadsheet で再現したモデル
 * * [事前準備]
 * 1. スプレッドシートを新規作成し，URLからIDを取得して SPREADSHEET_ID に設定．
 * 2. エディタ上部の関数選択から `initDB` を選んで実行（シートと初期データが作成されます）．
 * 3. 「デプロイ」>「新しいデプロイ」>「ウェブアプリ」として公開．
 */

const SPREADSHEET_ID = '1KJQ3RBZpuy6gZS5QkbB4XIdoYeMz3lz6nLmDX9iqi9o'; // ★要変更
const JWT_SECRET = 'secret'; // ★適当な文字列に変更

// ==========================================
// 1. ORM（Spreadsheet Database Layer） ※高速化（キャッシュ）対応版
// ==========================================
class Table {
  constructor(sheetName, headers) {
    this.sheetName = sheetName;
    this.headers = headers;
    this.sheet = null;
  }

  _getSheet() {
    if (!this.sheet) {
      const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
      this.sheet = ss.getSheetByName(this.sheetName);
      if (!this.sheet) {
        this.sheet = ss.insertSheet(this.sheetName);
        this.sheet.appendRow(this.headers);
      }
    }
    return this.sheet;
  }

  // ★ 追加：キャッシュを削除する機能
  _clearCache() {
    const cache = CacheService.getScriptCache();
    cache.remove('DB_CACHE_' + this.sheetName);
  }

  getAll() {
    // ★ 追加：まずはキャッシュ（一時保存データ）があるか確認する
    const cache = CacheService.getScriptCache();
    const cachedData = cache.get('DB_CACHE_' + this.sheetName);
    if (cachedData) {
      return JSON.parse(cachedData); // キャッシュがあればスプレッドシートを読まずに即返す！
    }

    // キャッシュがない場合のみ、スプレッドシートを読みに行く
    const sheet = this._getSheet();
    const data = sheet.getDataRange().getValues();
    if (data.length <= 1) return [];
    
    const currentHeaders = data[0];
    const result = data.slice(1).map((row, rowIndex) => {
      let obj = { _rowIndex: rowIndex + 2 };
      currentHeaders.forEach((h, i) => { obj[h] = row[i]; });
      return obj;
    });

    // ★ 追加：次回のために、読み込んだ結果をキャッシュに保存する（最大6時間）
    try {
      cache.put('DB_CACHE_' + this.sheetName, JSON.stringify(result), 21600);
    } catch (e) { /* データ量が多すぎて入らない場合は無視 */ }

    return result;
  }

  findBy(key, value) {
    return this.getAll().filter(item => item[key] == value);
  }

  findOne(key, value) {
    const results = this.findBy(key, value);
    return results.length > 0 ? results[0] : null;
  }

  findById(id) {
    return this.findOne('id', id);
  }

  insert(data) {
    const sheet = this._getSheet();
    const id = Utilities.getUuid();
    const now = new Date();
    data.id = id;
    if (this.headers.includes('created_at')) data.created_at = now;
    
    const row = this.headers.map(h => data[h] !== undefined ? data[h] : '');
    sheet.appendRow(row);
    
    this._clearCache(); // ★ 追加：データが新しくなったので古いキャッシュを捨てる
    return this.findById(id);
  }

  update(id, data) {
    const sheet = this._getSheet();
    const item = this.findById(id);
    if (!item) throw new Error("Record not found");

    this.headers.forEach((h, i) => {
      if (data[h] !== undefined && h !== 'id') {
        sheet.getRange(item._rowIndex, i + 1).setValue(data[h]);
      }
    });
    
    this._clearCache(); // ★ 追加：データが更新されたので古いキャッシュを捨てる
    return this.findById(id);
  }

  delete(id) {
    const sheet = this._getSheet();
    const item = this.findById(id);
    if (item) {
      sheet.deleteRow(item._rowIndex);
      this._clearCache(); // ★ 追加：データが削除されたので古いキャッシュを捨てる
      return true;
    }
    return false;
  }
}

const DB = {
  Users: new Table('Users', ['id', 'email', 'student_id', 'last_name', 'first_name', 'grade', 'dob', 'password_hash', 'is_admin', 'must_change_password', 'access_token', 'created_at', 'last_login_at']),
  Items: new Table('Items', ['id', 'category_id', 'code', 'product_name', 'manufacturer', 'model_number', 'stock', 'available', 'location', 'description', 'created_at']),
  Categories: new Table('Categories', ['id', 'name']),
  Loans: new Table('Loans', ['id', 'user_id', 'item_id', 'qty', 'purpose', 'loaned_at', 'due_at', 'returned_at']),
  Audit: new Table('Audit', ['id', 'created_at', 'action', 'entity', 'actor', 'meta'])
};

// ==========================================
// 2. ユーティリティ & 認証
// ==========================================
function hashPassword(password) {
  const digest = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, password + JWT_SECRET);
  return digest.map(b => (b < 0 ? b + 256 : b).toString(16).padStart(2, '0')).join('');
}

function generateToken() {
  return Utilities.getUuid();
}

function auditLog(action, entity, actor, meta = "") {
  DB.Audit.insert({ action: action, entity: entity, actor: actor, meta: JSON.stringify(meta) });
}

// 認証ミドルウェア相当
function getCurrentUser(req) {
  const token = req.headers.authorization || req.parameter.token;
  if (!token) throw { status: 401, message: "Not authenticated" };
  
  const actualToken = token.replace(/^Bearer\s/i, "");
  const user = DB.Users.findOne('access_token', actualToken);
  if (!user) throw { status: 401, message: "Invalid token" };
  return user;
}

function requireAdmin(user) {
  if (user.is_admin !== true && user.is_admin !== 'true') {
    throw { status: 403, message: "Admin only" };
  }
}

// ==========================================
// 3. ルーターとHTTPハンドラ (FastAPIエミュレータ)
// ==========================================
const routes = [];

// ルート登録用ヘルパー
function addRoute(method, pathRegex, handler) {
  routes.push({ method: method, regex: pathRegex, handler: handler });
}

function processRequest(method, path, req) {
  try {
    for (let route of routes) {
      if (route.method === method) {
        const match = path.match(route.regex);
        if (match) {
          req.pathParams = match.slice(1); // パスパラメータを抽出
          const result = route.handler(req);
          return ContentService.createTextOutput(JSON.stringify(result))
            .setMimeType(ContentService.MimeType.JSON);
        }
      }
    }
    throw { status: 404, message: "Route not found: " + method + " " + path };
  } catch (err) {
    const status = err.status || 500;
    return ContentService.createTextOutput(JSON.stringify({ detail: err.message || String(err) }))
      .setMimeType(ContentService.MimeType.JSON); // GASではHTTPステータスコードを自由に変更できないためボディに含める
  }
}

function doGet(e) {
  const path = e.parameter.path || '/';
  return processRequest('GET', path, { parameter: e.parameter, headers: {} });
}

function doPost(e) {
  let payload = {};
  try {
    if (e.postData && e.postData.contents) {
      payload = JSON.parse(e.postData.contents);
    }
  } catch (err) { /* Form urlencoded等の場合は無視 */ }
  
  // payload.method で PUT や DELETE をエミュレート (GAS制約回避)
  const method = payload._method || 'POST';
  const path = payload._path || e.parameter.path || '/';
  
  return processRequest(method, path, { 
    parameter: e.parameter, 
    payload: payload, 
    headers: { authorization: payload.token || e.parameter.token } 
  });
}

// ==========================================
// 4. コントローラ実装 (エンドポイント)
// ==========================================

// --- Auth ---
addRoute('POST', /^\/token$/, (req) => {
  const { username, password } = req.payload;
  const user = DB.Users.findOne('email', username);
  if (!user || user.password_hash !== hashPassword(password)) {
    throw { status: 401, message: "login failed" };
  }
  const token = generateToken();
  DB.Users.update(user.id, { access_token: token, last_login_at: new Date() });
  return { access_token: token, token_type: "bearer", must_change: user.must_change_password === 'true' };
});

addRoute('GET', /^\/me$/, (req) => {
  const user = getCurrentUser(req);
  delete user.password_hash; // セキュリティのため除外
  delete user.access_token;
  return user;
});

// --- Inventory (Items) ---
addRoute('GET', /^\/inventory$/, (req) => {
  const user = getCurrentUser(req);
  let items = DB.Items.getAll();
  const q = req.parameter.q;
  if (q) {
    items = items.filter(i => i.product_name.includes(q) || i.code.includes(q));
  }
  // カテゴリ名を結合
  const categories = DB.Categories.getAll();
  items = items.map(item => {
    const cat = categories.find(c => c.id == item.category_id);
    item.category_name = cat ? cat.name : null;
    return item;
  });
  return items; // 今回はページネーションを省略し全件返す（GASの簡略化）
});

addRoute('POST', /^\/admin\/items$/, (req) => {
  const admin = getCurrentUser(req);
  requireAdmin(admin);
  const data = req.payload;
  // コード自動採番ロジック (簡易版: ITEM-00X)
  if (!data.code) {
    const count = DB.Items.getAll().length + 1;
    data.code = `ITEM-${String(count).padStart(3, '0')}`;
  }
  const newItem = DB.Items.insert(data);
  auditLog("create", "item", admin.email, newItem.code);
  return newItem;
});

addRoute('DELETE', /^\/admin\/items\/([a-zA-Z0-9-]+)$/, (req) => {
  const admin = getCurrentUser(req);
  requireAdmin(admin);
  const itemId = req.pathParams[0];
  DB.Items.delete(itemId);
  auditLog("delete", "item", admin.email, itemId);
  return { ok: true };
});

// --- QR Code (Google Charts API) ---
addRoute('GET', /^\/items\/([a-zA-Z0-9-]+)\/qrcode\.png$/, (req) => {
  // ※認証を必須にする場合はここで getCurrentUser(req); を呼び出します．
  const itemId = req.pathParams[0];
  const item = DB.Items.findById(itemId);
  
  if (!item) throw { status: 404, message: "item not found" };
  
  // 埋め込むデータを構築
  const data = `ITEM:${item.id}:${item.code || ''}`;
  
  // Google Charts API のURLを生成（250x250ピクセル）
  const chartUrl = `https://chart.googleapis.com/chart?chs=250x250&cht=qr&chl=${encodeURIComponent(data)}`;
  
  // GASの仕様上，画像バイナリを直接返せないため，画像URLをJSONで返却します．
  return { ok: true, qr_url: chartUrl };
});

// --- Loans ---
addRoute('POST', /^\/loans\/me$/, (req) => {
  const user = getCurrentUser(req);
  const { item_code, qty = 1, days, purpose } = req.payload;
  const item = DB.Items.findOne('code', item_code);
  
  if (!item) throw { status: 404, message: "在庫が見つかりません" };
  if (parseInt(item.stock) < parseInt(qty) || item.available === 'false') {
    throw { status: 400, message: "在庫不足または貸出停止中" };
  }
  
  let dueAt = "";
  if (days) {
    const d = new Date();
    d.setDate(d.getDate() + parseInt(days));
    dueAt = d;
  }

  const loan = DB.Loans.insert({
    user_id: user.id,
    item_id: item.id,
    qty: qty,
    purpose: purpose,
    loaned_at: new Date(),
    due_at: dueAt
  });
  
  auditLog("create", "loan", user.email, `${item_code} x${qty}`);
  return { id: loan.id };
});

addRoute('POST', /^\/loans\/return$/, (req) => {
  const user = getCurrentUser(req);
  const { loan_id } = req.payload;
  const loan = DB.Loans.findById(loan_id);
  
  if (!loan) throw { status: 404, message: "loan not found" };
  if (user.is_admin !== 'true' && loan.user_id != user.id) {
    throw { status: 403, message: "not your loan" };
  }
  
  if (!loan.returned_at) {
    DB.Loans.update(loan_id, { returned_at: new Date() });
    auditLog("return", "loan", user.email, loan_id);
  }
  return { ok: true };
});

addRoute('GET', /^\/me\/loans$/, (req) => {
  const user = getCurrentUser(req);
  const activeOnly = req.parameter.active_only !== 'false';
  let loans = DB.Loans.findBy('user_id', user.id);
  
  if (activeOnly) {
    loans = loans.filter(l => !l.returned_at);
  }
  
  const items = DB.Items.getAll();
  return loans.map(l => {
    const item = items.find(i => i.id == l.item_id);
    return {
      id: l.id,
      item_name: item ? item.product_name : 'Unknown',
      item_code: item ? item.code : '',
      qty: l.qty,
      loaned_at: l.loaned_at,
      due_at: l.due_at,
      returned_at: l.returned_at
    };
  });
});

// ==========================================
// ルート（エンドポイント）群
// ==========================================

// カテゴリ一覧の取得
addRoute('GET', /^\/categories$/, (req) => {
  return DB.Categories.getAll();
});

// 管理者用：ユーザ一覧の取得
addRoute('GET', /^\/admin\/users$/, (req) => {
  const admin = getCurrentUser(req);
  requireAdmin(admin);
  return DB.Users.getAll().map(u => {
    delete u.password_hash;
    delete u.access_token;
    return u;
  });
});

// 管理者用：備品詳細の取得
addRoute('GET', /^\/admin\/items\/([a-zA-Z0-9-]+)$/, (req) => {
  const item = DB.Items.findById(req.pathParams[0]);
  if (!item) throw { status: 404, message: "Item not found" };
  const cat = DB.Categories.findById(item.category_id);
  item.category_name = cat ? cat.name : null;
  return item;
});

// 管理者用：全貸出一覧の取得
addRoute('GET', /^\/loans$/, (req) => {
  const admin = getCurrentUser(req);
  requireAdmin(admin);
  const activeOnly = req.parameter.active_only !== 'false';
  let loans = DB.Loans.getAll();
  if (activeOnly) loans = loans.filter(l => !l.returned_at);
  
  const items = DB.Items.getAll();
  const users = DB.Users.getAll();
  return loans.map(l => {
    const item = items.find(i => i.id == l.item_id) || {};
    const user = users.find(u => u.id == l.user_id) || {};
    return {
      ...l,
      item_name: item.product_name || '不明',
      item_code: item.code || '',
      borrower: (user.last_name || '') + ' ' + (user.first_name || '')
    };
  });
});

// 管理者用：監査ログの取得
addRoute('GET', /^\/admin\/audit$/, (req) => {
  const admin = getCurrentUser(req);
  requireAdmin(admin);
  return DB.Audit.getAll().reverse();
});

// 一般：備品詳細の取得 (GET /items/{id})
addRoute('GET', /^\/items\/([a-zA-Z0-9-]+)$/, (req) => {
  getCurrentUser(req); // ログイン必須
  const itemId = req.pathParams[0];
  const item = DB.Items.findById(itemId);
  if (!item) throw { status: 404, message: "Item not found" };
  
  const cat = DB.Categories.findById(item.category_id);
  item.category_name = cat ? cat.name : null;
  return item;
});

// 一般：備品の現在の借用者取得 (GET /items/{id}/borrowers)
addRoute('GET', /^\/items\/([a-zA-Z0-9-]+)\/borrowers$/, (req) => {
  getCurrentUser(req); // ログイン必須
  const itemId = req.pathParams[0];
  const activeLoans = DB.Loans.findBy('item_id', itemId).filter(l => !l.returned_at);
  
  const users = DB.Users.getAll();
  return activeLoans.map(l => {
    const u = users.find(user => user.id == l.user_id) || {};
    return {
      loaned_at: l.loaned_at,
      user_name: (u.last_name || '') + ' ' + (u.first_name || '不明')
    };
  });
});

// 一般：自身のパスワード変更 (POST /me/password)
addRoute('POST', /^\/me\/password$/, (req) => {
  const user = getCurrentUser(req);
  const { old_password, new_password } = req.payload;
  
  if (user.password_hash !== hashPassword(old_password)) {
    throw { status: 400, message: "現在のパスワードが間違っています" };
  }
  
  DB.Users.update(user.id, {
    password_hash: hashPassword(new_password),
    must_change_password: false
  });
  return { ok: true };
});

// --- 追加：フロントエンド高速化のための全データ一括同期ルート（改・管理者対応版） ---
addRoute('GET', /^\/sync$/, (req) => {
  const user = getCurrentUser(req); // ログイン必須
  const isAdmin = (user.is_admin === true || user.is_admin === 'true');
  
  const items = DB.Items.getAll();
  const categories = DB.Categories.getAll();
  let loans = DB.Loans.getAll();
  
  // 一般ユーザーの場合は自分の貸出のみに絞る（管理者は全件取得）
  if (!isAdmin) {
    loans = loans.filter(l => l.user_id == user.id);
  }

  const itemsWithCat = items.map(item => {
    const cat = categories.find(c => c.id == item.category_id);
    item.category_name = cat ? cat.name : null;
    return item;
  });

  const users = DB.Users.getAll();
  const loansWithNames = loans.map(l => {
    const item = items.find(i => i.id == l.item_id) || {};
    const u = users.find(u => u.id == l.user_id) || {};
    return {
      ...l,
      item_name: item.product_name || '不明',
      item_code: item.code || '',
      borrower: (u.last_name || '') + ' ' + (u.first_name || '')
    };
  });

  // ベースとなる返却データ（一般・管理者共通）
  let response = {
    items: itemsWithCat,
    categories: categories,
    loans: loansWithNames,
    timestamp: new Date().getTime()
  };

  // 管理者の場合のみ、全ユーザー情報と監査ログを追加でレスポンスに含める
  if (isAdmin) {
    response.users = users.map(u => {
      delete u.password_hash; // パスワード情報は絶対に送らない
      delete u.access_token;
      return u;
    });
    response.audit = DB.Audit.getAll().reverse();
  }

  return response;
});

// ==========================================
// 5. 初期化スクリプト (Pythonの create_db_and_seed 相当)
// ==========================================
function initDB() {
  // シート作成とヘッダー初期化を呼び出す（Tableクラス内で自動実行）
  Object.values(DB).forEach(table => table._getSheet());
  
  // シードデータ投入（管理者アカウント）
  const adminEmail = "admin@example.com";
  if (!DB.Users.findOne('email', adminEmail)) {
    DB.Users.insert({
      email: adminEmail,
      password_hash: hashPassword("AdminPass!1"),
      is_admin: true,
      last_name: "Admin",
      first_name: "User",
      must_change_password: false
    });
  }
  
  // シードデータ（カテゴリと備品）
  if (DB.Categories.getAll().length === 0) {
    const cat = DB.Categories.insert({ name: "周辺機器" });
    DB.Items.insert({
      category_id: cat.id,
      code: "PERI-001",
      product_name: "USB-C ハブ",
      manufacturer: "UGREEN",
      stock: 5,
      available: true,
      location: "棚A-1"
    });
  }
  
  Logger.log("DB Initialization completed.");
}