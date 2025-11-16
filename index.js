require('dotenv').config();
const fs = require('fs');
const csv = require('csv-parser');
const mysql = require('mysql2/promise');
const cron = require('node-cron');
const { processarAtivos } = require('./processa-ativos');
const { atualizarGrafico } = require('./atualizar-grafico');

const dbConfig = {
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  database: process.env.DB_NAME
};

const cotacoesPath = 'C:/Users/cealbert/Downloads/Macro_Watchlist_MACROEXCEL.csv';
const minFerrPath = 'C:/Users/cealbert/Downloads/MinFerr.csv';

function readCSV(filePath) {
  return new Promise((resolve, reject) => {
    const results = [];
    fs.createReadStream(filePath)
      .pipe(csv({ separator: ',' }))
      .on('data', (data) => results.push(data))
      .on('end', () => resolve(results))
      .on('error', (err) => reject(err));
  });
}

async function readCotacoes() {
  const rows = await readCSV(cotacoesPath);
  return rows.map(r => {
    const row = {};
    for (const key in r) {
      row[key.replace(/"/g, '').trim()] = r[key];
    }
    return {
      nome: (row['Nome'] || '').trim(),
      codigo: (row['Códigos'] || '').trim().toUpperCase(),
      preco: parseFloat((row['Prévio'] || '0').replace(',', '.')) || 0,
      variacao: parseFloat((row['Var%'] || '0').replace('%', '').replace(',', '.')) || 0,
      tipo_contagem: 'simple'
    };
  });
}

async function readMinFerr() {
  const rows = await readCSV(minFerrPath);
  const valorStr = rows[0]['coluna1'] || '0';
  return parseFloat(valorStr.replace('%', '').replace(',', '.')) || 0;
}

async function calcularAtivosCompostos(cotacoes, conn) {
  const [compostos] = await conn.execute('SELECT codigo, componentes FROM ativos_compostos');
  const novosAtivos = [];
  const codigosComponentes = new Set();

  for (const comp of compostos) {
    const lista = comp.componentes.split(',');
    const variacoes = lista.map(cod => {
      const ativo = cotacoes.find(c => c.codigo === cod.trim().toUpperCase());
      return ativo ? ativo.variacao : 0;
    });
    const media = variacoes.reduce((a, b) => a + b, 0) / variacoes.length;

    novosAtivos.push({
      nome: comp.codigo,
      codigo: comp.codigo,
      preco: 0,
      variacao: media,
      tipo_contagem: 'simple'
    });

    for (const cod of lista) {
      const ativoOriginal = cotacoes.find(c => c.codigo === cod.trim().toUpperCase());
      if (ativoOriginal) {
        codigosComponentes.add(ativoOriginal.codigo);
        novosAtivos.push({
          ...ativoOriginal,
          tipo_contagem: 'composto'
        });
      }
    }
  }

  const cotacoesFiltradas = cotacoes.filter(c => !codigosComponentes.has(c.codigo));
  return { ativosCompostos: novosAtivos, cotacoesFiltradas };
}

async function saveCotacoes(cotacoes, conn) {
  const timestamp = new Date();
  for (const c of cotacoes) {
    await conn.execute(
      'INSERT INTO cotacoes (nome, codigo, preco, variacao, timestamp) VALUES (?, ?, ?, ?, ?)',
      [c.nome || null, c.codigo || null, c.preco, c.variacao, timestamp]
    );
  }
}

async function saveMinFerr(valor, conn, todosAtivos) {
  const timestamp = new Date();
  const codigo = 'MINFERR';
  const nome = 'MinFerr';

  await conn.execute(
    'INSERT INTO minfer (valor, timestamp, codigo) VALUES (?, ?, ?)',
    [valor, timestamp, codigo]
  );

  await conn.execute(
    'INSERT INTO cotacoes (nome, codigo, preco, variacao, timestamp) VALUES (?, ?, ?, ?, ?)',
    [nome, codigo, 0, valor, timestamp]
  );

  todosAtivos.push({
    nome,
    codigo,
    preco: 0,
    variacao: valor,
    tipo_contagem: 'simple'
  });
}

async function processarEstrangeiro(conn, todosAtivos) {
  const [rows] = await conn.execute('SELECT diferenca FROM estrangeiro ORDER BY timestamp DESC LIMIT 2');
  if (rows.length < 1) return;

  const atual = parseFloat(rows[0].diferenca);
  const anterior = rows.length > 1 ? parseFloat(rows[1].diferenca) : 0;
  const acum = atual - anterior;

  let sinal = 'Neutro';
  if (acum < -1000) sinal = 'Queda';
  else if (acum > 1000) sinal = 'Alta';

  await conn.execute(
    'INSERT INTO ativos (tipo, nome, sinal, variacao, timestamp, tipo_contagem) VALUES (?, ?, ?, ?, ?, ?)',
    ['seguranca', 'Estrangeiro', sinal, acum, new Date(), 'simple']
  );

  await conn.execute(
    'INSERT INTO cotacoes (nome, codigo, preco, variacao, timestamp) VALUES (?, ?, ?, ?, ?)',
    ['Estrangeiro', 'ESTRANGEIRO', 0, acum, new Date()]
  );

  todosAtivos.push({
    nome: 'Estrangeiro',
    codigo: 'ESTRANGEIRO',
    preco: 0,
    variacao: acum,
    tipo_contagem: 'simple'
  });
}

async function executarFluxo() {
  try {
    console.log('Iniciando atualização...');
    const conn = await mysql.createConnection(dbConfig);

    const cotacoes = await readCotacoes();
    await saveCotacoes(cotacoes, conn);

    const { ativosCompostos, cotacoesFiltradas } = await calcularAtivosCompostos(cotacoes, conn);
    const todosAtivos = [...cotacoesFiltradas, ...ativosCompostos];

    const minFerrValor = await readMinFerr();
    await saveMinFerr(minFerrValor, conn, todosAtivos);

    await processarEstrangeiro(conn, todosAtivos);

    await processarAtivos(todosAtivos);
    await atualizarGrafico();

    await conn.end();
    console.log('Atualização concluída.');
  } catch (err) {
    console.error('Erro no fluxo:', err);
  }
}

let isRunning = false;
cron.schedule('*/2 * * * *', async () => {
  if (isRunning) {
    console.log('Fluxo anterior ainda em execução. Ignorando nova execução.');
    return;
  }
  isRunning = true;
  try {
    await executarFluxo();
  } catch (err) {
    console.error('Erro no cron:', err);
  } finally {
    isRunning = false;
  }
});

(async () => {
  await executarFluxo();
})();
