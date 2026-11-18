/* 前端公共逻辑：账户列表 / 持仓明细 */
var PF = (function () {
  var numberFormat = new Intl.NumberFormat('zh-CN', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  });

  function esc(text) {
    return String(text === null || text === undefined ? '' : text)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function fmt(value) {
    if (value === null || value === undefined || Number.isNaN(value)) {
      return '<span class="dim">-</span>';
    }
    return numberFormat.format(value);
  }

  /* 盈利红、亏损绿（0 与空不着色） */
  function numClass(value) {
    if (value === null || value === undefined || value === 0) return '';
    return value > 0 ? 'up' : 'down';
  }

  function sideClass(side) {
    if (side === '买') return 'up';
    if (side === '卖') return 'down';
    return '';
  }

  function fetchJSON(url) {
    return fetch(url, { cache: 'no-store' }).then(function (resp) {
      if (!resp.ok) throw new Error('HTTP ' + resp.status);
      return resp.json();
    });
  }

  function renderMeta(el, data) {
    var parts = [];
    if (data.source) {
      parts.push('<span>来源 ' + esc(data.source) + '</span>');
    } else {
      parts.push('<span class="dim">未找到数据文件</span>');
    }
    if (data.has_source) {
      parts.push(data.cached
        ? '<span class="badge hit">缓存命中</span>'
        : '<span class="badge miss">已重建缓存</span>');
    }
    if (data.generated_at) {
      parts.push('<span>整理于 ' + esc(data.generated_at) + '</span>');
    }
    el.innerHTML = parts.join('');
  }

  function sum(list, key) {
    var total = 0;
    list.forEach(function (item) {
      total += Number(item[key]) || 0;
    });
    return total;
  }

  /* ---------- 列表页：账户权益 ---------- */
  function loadAccounts() {
    var rowsEl = document.getElementById('rows');
    var footEl = document.getElementById('foot');
    var metaEl = document.getElementById('meta');

    fetchJSON('/api/accounts').then(function (data) {
      renderMeta(metaEl, data);
      var accounts = data.accounts || [];
      if (!accounts.length) {
        rowsEl.innerHTML = '<tr><td colspan="5" class="empty">暂无账户数据</td></tr>';
        footEl.innerHTML = '';
        return;
      }

      rowsEl.innerHTML = accounts.map(function (item) {
        var accountCell = item.has_position
          ? '<a class="acc" href="/position/' + encodeURIComponent(item.account) + '">' + esc(item.account) + '</a>'
          : esc(item.account) + ' <span class="dim" title="没有对应的持仓文件">–</span>';
        return '<tr>' +
          '<td>' + accountCell + '</td>' +
          '<td class="num">' + fmt(item.equity) + '</td>' +
          '<td class="num">' + fmt(item.margin) + '</td>' +
          '<td class="num ' + numClass(item.hold_profit) + '">' + fmt(item.hold_profit) + '</td>' +
          '<td class="num ' + numClass(item.close_profit) + '">' + fmt(item.close_profit) + '</td>' +
          '</tr>';
      }).join('');

      var total = data.total;
      footEl.innerHTML = total
        ? '<tr>' +
            '<td>合 计</td>' +
            '<td class="num">' + fmt(total.equity) + '</td>' +
            '<td class="num">' + fmt(total.margin) + '</td>' +
            '<td class="num ' + numClass(total.hold_profit) + '">' + fmt(total.hold_profit) + '</td>' +
            '<td class="num ' + numClass(total.close_profit) + '">' + fmt(total.close_profit) + '</td>' +
          '</tr>'
        : '';
    }).catch(function (err) {
      metaEl.textContent = '加载失败';
      rowsEl.innerHTML = '<tr><td colspan="5" class="error">加载失败：' + esc(err.message) + '</td></tr>';
    });
  }

  /* ---------- 详情页：单账号持仓 ---------- */
  function loadPositions(account) {
    var rowsEl = document.getElementById('rows');
    var footEl = document.getElementById('foot');
    var metaEl = document.getElementById('meta');

    fetchJSON('/api/positions/' + encodeURIComponent(account)).then(function (data) {
      renderMeta(metaEl, data);
      var list = data.positions || [];
      if (!list.length) {
        rowsEl.innerHTML = '<tr><td colspan="8" class="empty">' +
          (data.has_source ? '该账号当前没有持仓' : '未找到该账号的持仓文件') +
          '</td></tr>';
        footEl.innerHTML = '';
        return;
      }

      rowsEl.innerHTML = list.map(function (item) {
        return '<tr>' +
          '<td>' + esc(item['合约ID']) + '</td>' +
          '<td class="' + sideClass(item['买卖']) + '">' + esc(item['买卖']) + '</td>' +
          '<td class="num ' + numClass(item['逐日浮盈']) + '">' + fmt(item['逐日浮盈']) + '</td>' +
          '<td class="num ' + numClass(item['逐笔浮盈']) + '">' + fmt(item['逐笔浮盈']) + '</td>' +
          '<td class="num">' + fmt(item['今仓']) + '</td>' +
          '<td class="num">' + fmt(item['持仓量']) + '</td>' +
          '<td class="num">' + fmt(item['逐笔开仓均价']) + '</td>' +
          '<td class="num">' + fmt(item['占用保证金']) + '</td>' +
          '</tr>';
      }).join('');

      footEl.innerHTML = '<tr>' +
        '<td colspan="2">合 计</td>' +
        '<td class="num ' + numClass(sum(list, '逐日浮盈')) + '">' + fmt(sum(list, '逐日浮盈')) + '</td>' +
        '<td class="num ' + numClass(sum(list, '逐笔浮盈')) + '">' + fmt(sum(list, '逐笔浮盈')) + '</td>' +
        '<td class="num">' + fmt(sum(list, '今仓')) + '</td>' +
        '<td class="num">' + fmt(sum(list, '持仓量')) + '</td>' +
        '<td class="num"></td>' +
        '<td class="num">' + fmt(sum(list, '占用保证金')) + '</td>' +
        '</tr>';
    }).catch(function (err) {
      metaEl.textContent = '加载失败';
      rowsEl.innerHTML = '<tr><td colspan="8" class="error">加载失败：' + esc(err.message) + '</td></tr>';
    });
  }

  return {
    loadAccounts: loadAccounts,
    loadPositions: loadPositions
  };
})();
