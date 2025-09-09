const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function gql(url, token, query, variables = {}) {
  const r = await fetch(`${url}/admin/api/${process.env.SHOPIFY_API_VERSION || '2024-07'}/graphql.json`, {
    method: 'POST',
    headers: {
      'X-Shopify-Access-Token': token,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({ query, variables })
  });
  const text = await r.text();
  if (!r.ok) throw new Error(`GraphQL ${r.status}: ${text}`);
  const json = JSON.parse(text);
  if (json.errors) throw new Error(`GraphQL errors: ${JSON.stringify(json.errors)}`);
  return json.data;
}

async function getAnyLocationIdREST(store, headers) {
  const r = await fetch(`${store}/admin/api/${process.env.SHOPIFY_API_VERSION || '2024-07'}/locations.json`, { headers });
  const text = await r.text();
  if (!r.ok) throw new Error(`GET locations -> ${r.status}: ${text}`);
  const { locations = [] } = JSON.parse(text);
  if (!locations.length) throw new Error('No hay locations en la tienda');
  return locations[0].id;
}

exports.handler = async (event) => {
  try {
    const STORE   = process.env.SHOPIFY_STORE;        // ej: https://tutienda.myshopify.com
    const TOKEN   = process.env.SHOPIFY_API_KEY;      // shpat_...
    const VERSION = process.env.SHOPIFY_API_VERSION || '2024-07';
    const FIXED_LOCATION_ID = process.env.LOCATION_ID; // opcional

    const missing = [];
    if (!STORE) missing.push('SHOPIFY_STORE');
    if (!TOKEN) missing.push('SHOPIFY_API_KEY');
    try { new URL(STORE); } catch { missing.push('SHOPIFY_STORE (URL inválida; incluye https://)'); }
    if (missing.length) return { statusCode: 500, body: JSON.stringify({ error: 'Faltan variables', missing }) };

    const qs = event.queryStringParameters || {};
    const productIdFilter = qs.productId ? Number(qs.productId) : null;
    const handleFilter    = qs.handle || null;
    const force           = qs.force === '1' || qs.force === 'true';
    const wantDebug       = qs.debug === '1' || qs.debug === 'true';

    // Fecha de "hoy" en Lima (YYYY-MM-DD)
    const todayYMD = new Intl.DateTimeFormat('en-CA', {
      timeZone: 'America/Lima', year: 'numeric', month: '2-digit', day: '2-digit'
    }).format(new Date());

    const headers = { 'X-Shopify-Access-Token': TOKEN, 'Content-Type': 'application/json' };
    const updated = [];
    const skipped = [];
    const debug   = [];

    // === Obtener location ===
    let locationId = FIXED_LOCATION_ID ? Number(FIXED_LOCATION_ID) : null;
    if (!locationId) locationId = await getAnyLocationIdREST(STORE, headers);

    // === Estrategias de selección ===
    // 1) Producto puntual por ID
    if (productIdFilter) {
      const data = await gql(STORE, TOKEN, `
        query($id: ID!) {
          product(id: $id) {
            id title handle
            variants(first: 1) { nodes { id legacyResourceId inventoryItem { id legacyResourceId } inventoryManagement } }
            metafield(namespace: "custom", key: "fecha_disponibilidad") { value }
            metafieldStock: metafield(namespace: "custom", key: "stock_programado") { value }
          }
        }`, { id: `gid://shopify/Product/${productIdFilter}` });

      if (!data.product) return { statusCode: 200, body: JSON.stringify({ message: 'Run OK', note: 'Producto no encontrado', todayYMD, updated, skipped }) };
      const products = [data.product];
      await processProducts({ products, STORE, TOKEN, VERSION, headers, todayYMD, force, locationId, updated, skipped, debug, wantDebug });
      return { statusCode: 200, body: JSON.stringify({ message: 'Run OK', todayYMD, filters: { productIdFilter, force }, counts: { updated: updated.length, skipped: skipped.length }, updated, skipped, debug }, null, 2) };
    }

    // 2) Producto puntual por handle
    if (handleFilter) {
      const data = await gql(STORE, TOKEN, `
        query($query:String!) {
          products(first: 1, query: $query) {
            nodes {
              id title handle
              variants(first: 1) { nodes { id legacyResourceId inventoryItem { id legacyResourceId } inventoryManagement } }
              metafield(namespace: "custom", key: "fecha_disponibilidad") { value }
              metafieldStock: metafield(namespace: "custom", key: "stock_programado") { value }
            }
          }
        }`, { query: `handle:${handleFilter}` });

      const products = data?.products?.nodes || [];
      await processProducts({ products, STORE, TOKEN, VERSION, headers, todayYMD, force, locationId, updated, skipped, debug, wantDebug });
      return { statusCode: 200, body: JSON.stringify({ message: 'Run OK', todayYMD, filters: { handleFilter, force }, counts: { updated: updated.length, skipped: skipped.length }, updated, skipped, debug }, null, 2) };
    }

    // 3) MASIVO: SOLO productos con ambos metafields definidos (filtro GraphQL)
    // Nota: la sintaxis de búsqueda soporta "metafield:namespace.key:*" para "existe"
    let hasNextPage = true;
    let cursor = null;
    let totalFetched = 0;

    while (hasNextPage) {
      const data = await gql(STORE, TOKEN, `
        query($after:String) {
          products(
            first: 100,
            after: $after,
            query: "metafield:custom.fecha_disponibilidad:* AND metafield:custom.stock_programado:*"
          ) {
            pageInfo { hasNextPage endCursor }
            nodes {
              id title handle
              variants(first: 1) {
                nodes { id legacyResourceId inventoryItem { id legacyResourceId } inventoryManagement }
              }
              metafield(namespace: "custom", key: "fecha_disponibilidad") { value }
              metafieldStock: metafield(namespace: "custom", key: "stock_programado") { value }
            }
          }
        }`, { after: cursor });

      const products = data?.products?.nodes || [];
      totalFetched += products.length;

      await processProducts({ products, STORE, TOKEN, VERSION, headers, todayYMD, force, locationId, updated, skipped, debug, wantDebug });

      hasNextPage = data?.products?.pageInfo?.hasNextPage || false;
      cursor      = data?.products?.pageInfo?.endCursor || null;

      // pacing por cuota GraphQL
      await sleep(400);
    }

    return {
      statusCode: 200,
      body: JSON.stringify({
        message: 'Run OK',
        todayYMD,
        counts: { totalFetched, updated: updated.length, skipped: skipped.length },
        updated, skipped, debug
      }, null, 2)
    };

  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err?.message || String(err) }) };
  }
};

async function processProducts({ products, STORE, TOKEN, VERSION, headers, todayYMD, force, locationId, updated, skipped, debug, wantDebug }) {
  for (const p of products) {
    const fecha = p?.metafield?.value ? String(p.metafield.value).slice(0, 10) : null;
    const stock = p?.metafieldStock?.value ? parseInt(p.metafieldStock.value, 10) : 0;

    if (wantDebug) {
      debug.push({ productId: p.id, handle: p.handle, fecha, stock });
    }

    if (!force && (!fecha || fecha !== todayYMD)) {
      skipped.push({ productId: p.id, reason: 'date_mismatch_or_empty', fecha, todayYMD, stock });
      continue;
    }
    if (!stock || stock < 0) {
      skipped.push({ productId: p.id, reason: 'invalid_quantity', stock });
      continue;
    }

    const v = p?.variants?.nodes?.[0];
    if (!v) {
      skipped.push({ productId: p.id, reason: 'no_variant' });
      continue;
    }

    // Asegurar inventory_management = SHOPIFY en la variante (REST rápido)
    if (v.inventoryManagement !== 'SHOPIFY') {
      const r = await fetch(`${STORE}/admin/api/${VERSION}/variants/${v.legacyResourceId}.json`, {
        method: 'PUT', headers,
        body: JSON.stringify({ variant: { id: v.legacyResourceId, inventory_management: 'shopify' } })
      });
      if (!r.ok) {
        skipped.push({ productId: p.id, variantId: v.legacyResourceId, reason: 'enable_inventory_management_failed', text: await r.text() });
        continue;
      }
    }

    // Setear inventario en la location
    const setRes = await fetch(`${STORE}/admin/api/${VERSION}/inventory_levels/set.json`, {
      method: 'POST', headers,
      body: JSON.stringify({
        location_id: locationId,
        inventory_item_id: v.inventoryItem.legacyResourceId,
        available: stock
      })
    });

    if (setRes.ok) {
      updated.push({
        productId: p.id,
        handle: p.handle,
        variantId: v.legacyResourceId,
        inventoryItemId: v.inventoryItem.legacyResourceId,
        locationId,
        setTo: stock,
        fecha: force ? '(FORCED)' : todayYMD
      });
    } else {
      skipped.push({ productId: p.id, variantId: v.legacyResourceId, reason: 'inventory_set_failed', text: await setRes.text() });
    }

    await sleep(250); // pacing REST
  }
}
