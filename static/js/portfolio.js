
function today_price_get(obj) {
  const price = document.getElementsByName("price");
  const ticker = document.getElementsByName("ticker");
  var Num = obj.parentNode.parentNode.rowIndex-1;
  
  console.log(ticker[Num].value)
}

function today_price_add_get(obj) {
  const price = document.getElementsByName("price");
  const ticker = document.getElementsByName("ticker");
  var Num = obj.parentNode.parentNode.parentNode.rowIndex-1;
  console.log(ticker[Num].value)
}

function ADDRow_K() {
  var No_K = 2
  var cnt = 2;
  var tbody_K = document.getElementById('tbody_K');
  var row = tbody_K.insertRow(tbody_K.rows.length); // 하단에 추가
  var cell0 = row.insertCell(0);
  var cell1 = row.insertCell(1);
  var cell2 = row.insertCell(2);
  var cell3 = row.insertCell(3);
  var cell4 = row.insertCell(4);
  var cell5 = row.insertCell(5);
  var cell6 = row.insertCell(6);
  
  cell0.innerHTML = '<center><strong>'+No_K+'<strong></center>';
  cell1.innerHTML = '<input class="btn btn-outline-dark btn-sm" aria-label="Default select example" type="text" list="list" name="ticker"/><datalist class="ticker'+cnt+'" id ="list"></datalist>';
  for(let i = 0; i<data['K_company'].length; i++){
    var ticker_datalist = document.querySelector(".ticker"+cnt)
    var option = document.createElement('option');
    var text = document.createTextNode(data['K_company']+data['K_code'][i]);
    option.appendChild(text);
    ticker_datalist.appendChild(option);
  }
  cnt++;
  
  cell2.innerHTML = '<select class="btn btn-outline-dark btn-sm" aria-label="Default select example" name="order_state"><option selected>매수</option><option value="매도">매도</option><option value="예약">예약</option></select>';
  cell3.innerHTML = '<input class="rounded-3 border-1 btn-outline-dark" type="text" size="15" name="stock_cnt">';
  cell4.innerHTML = '<input class="rounded-3 border-1 btn-outline-dark" type="text" size="15" name="price">';
  cell5.innerHTML = '<center><input type="button" value="현재 주가로" class="btn btn-outline-success btn-sm" onclick=\'today_price_add_get(this);\'></center>';
  cell6.innerHTML = '<center><input type="button" value="한 줄 삭제" class="btn btn-outline-danger btn-sm" name="clear_row" onclick=\'clear_row_del(this);\'></center>';
  No_K++;
}

function ADDRow_F() {
  var No_F = 2;
  var cnt = 2;
  var tbody_F = document.getElementById('tbody_F');
  var row = tbody_F.insertRow(tbody_F.rows.length); // 하단에 추가
  var cell0 = row.insertCell(0);
  var cell1 = row.insertCell(1);
  var cell2 = row.insertCell(2);
  var cell3 = row.insertCell(3);
  var cell4 = row.insertCell(4);
  var cell5 = row.insertCell(5);
  var cell6 = row.insertCell(6);
  
  cell0.innerHTML = '<center><strong>'+No_F+'<strong></center>';
  cell1.innerHTML = '<input class="btn btn-outline-dark btn-sm" aria-label="Default select example" type="text" list="list_F" name="ticker"/><datalist class="ticker'+cnt+'" id ="list_F"></datalist>';
  for(let i = 0; i<data['F_company'].length; i++){
    var ticker_datalist = document.querySelector(".ticker"+cnt)
    var option = document.createElement('option');
    var text = document.createTextNode(data['F_company']+data['F_code'][i]);
    option.appendChild(text);
    ticker_datalist.appendChild(option);
  }
  cnt++;
  
  cell2.innerHTML = '<select class="btn btn-outline-dark btn-sm" aria-label="Default select example" name="order_state"><option selected>매수</option><option value="매도">매도</option><option value="예약">예약</option></select>';
  cell3.innerHTML = '<input class="rounded-3 border-1 btn-outline-dark" type="text" size="15" name="stock_cnt">';
  cell4.innerHTML = '<input class="rounded-3 border-1 btn-outline-dark" type="text" size="15" name="price">';
  cell5.innerHTML = '<center><input type="button" value="현재 주가로" class="btn btn-outline-success btn-sm" onclick=\'today_price_add_get(this);\'></center>';
  cell6.innerHTML = '<center><input type="button" value="한 줄 삭제" class="btn btn-outline-danger btn-sm" name="clear_row" onclick=\'clear_row_del(this);\'></center>';
  No_F++;
}

function clear_row_del(obj) {
  if (No_K==1){
    alert('더이상 지울 수 없습니다.');
    history.back(-1);
  } else{
    var tr = obj.parentNode.parentNode;
    tr.parentNode.remove(tr);
    No_K -=1;
  }
  cnt -=1;
}

var prices = document.getElementsByClassName('ticker')
for(var i=0;i<prices.length;i++){
  console.log(prices.item(i).value)
}

console.log()