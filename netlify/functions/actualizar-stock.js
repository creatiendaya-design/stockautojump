// Node 18 en Netlify ya tiene fetch global.

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
function ymdInTZ(date, tz) {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: tz, year: 'numeric', month: '2-digit', day: '2-digit'
  }).format(date); // YYYY-MM-DD
}

async function gql(store, token, query, variables = {}) {
  const version = process.env.SHOPIFY_API_VERSION || '2024-07';
  const r = await fetch(`${store}/admin/api/${version}/graphql.json`, {
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
  if (json.errors) throw new Error(`Errores de GraphQL: ${JSON.stringify(json.errors)}`);
  return json.data;
}

async function getAnyLocationIdREST(store, headers) {
  const version = process.env.SHOPIFY_API_VERSION || '2024-07';
  const r = await fetch(`${store}/admin/api/${version}/locations.json`, { headers });
  const text = await r.text();
  if (!r.ok) throw new Error(`GET locations -> ${r.status}: ${text}`);
  const { locations = [] } = JSON.parse(text);
  if (!locations.length) throw new Error('No hay locations en la tienda');
  return locations[0].id;
}

async function putJSON(url, headers, body) {
  const r = await fetch(url, { method: 'PUT', headers, body: JSON.stringify(body) });
  const text = await r.text();
  if (!r.ok) throw new Error(`PUT ${url} -> ${r.status}: ${text}`);
  return JSON.parse(text);
}

exports.handler = async (event) => {
  try {
    const STORE   = process.env.SHOPIFY_STORE;        // https://tu-tienda.myshopify.com
    const TOKEN   = process.env.SHOPIFY_API_KEY;      // shpat_...
    const VERSION = process.env.SHOPIFY_API_VERSION || '2024-07';
    const DEFAULT_TZ = process.env.TIMEZONE || 'America/New_York';
    const FIXED_LOCATION_ID = process.env.LOCATION_ID; // opcional

    const missing = [];
    if (!STORE) missing.push('SHOPIFY_STORE');
    if (!TOKEN) missing.push('SHOPIFY_API_KEY');
    try { new URL(STORE); } catch { missing.push('SHOPIFY_STORE (URL inválida; incluye https://)'); }
    if (missing.length) {
      return { statusCode: 500, body: JSON.stringify({ error: 'Faltan variables', missing }) };
    }

    const qs = event.queryStringParameters || {};
    const productIdFilter = qs.productId ? Number(qs.productId) : null;
    const handleFilter    = qs.handle || null;
    const force           = qs.force === '1' || qs.force === 'true';
    const wantDebug       = qs.debug === '1' || qs.debug === 'true';

    const headers = {
      'X-Shopify-Access-Token': TOKEN,
      'Content-Type': 'application/json'
    };

    const updated = [];
    const skipped = [];
    const debug   = [];

    // === Location ===
    let locationId = FIXED_LOCATION_ID ? Number(FIXED_LOCATION_ID) : null;
    if (!locationId) locationId = await getAnyLocationIdREST(STORE, headers);

    // === Selectores ===
    // 1) Por ID
    if (productIdFilter) {
      const data = await gql(STORE, TOKEN, `
        query($id: ID!) {
          product(id: $id) {
            id title handle
            variants(first: 1) {
              nodes {
                id
                legacyResourceId
                inventoryItem { id legacyResourceId tracked }
              }
            }
            metafield(namespace: "custom", key: "fecha_disponibilidad") { value }
            metafieldStock: metafield(namespace: "custom", key: "stock_programado") { value }
            metafieldTZ:   metafield(namespace: "custom", key: "timezone") { value }
          }
        }`, { id: `gid://shopify/Product/${productIdFilter}` });

      if (!data.product) {
        return { statusCode: 200, body: JSON.stringify({ message: 'Run OK', note: 'Producto no encontrado', updated, skipped, debug }) };
      }

      await processProducts({
        products: [data.product],
        STORE, VERSION, headers,
        defaultTZ: DEFAULT_TZ, force, locationId,
        updated, skipped, debug, wantDebug
      });

      return {
        statusCode: 200,
        body: JSON.stringify({
          message: 'Run OK (single ID)',
          filters: { productIdFilter, force },
          counts: { updated: updated.length, skipped: skipped.length },
          updated, skipped, debug
        }, null, 2)
      };
    }

    // 2) Por handle
    if (handleFilter) {
      const data = await gql(STORE, TOKEN, `
        query($query:String!) {
          products(first: 1, query: $query) {
            nodes {
              id title handle
              variants(first: 1) {
                nodes {
                  id
                  legacyResourceId
                  inventoryItem { id legacyResourceId tracked }
                }
              }
              metafield(namespace: "custom", key: "fecha_disponibilidad") { value }
              metafieldStock: metafield(namespace: "custom", key: "stock_programado") { value }
              metafieldTZ:   metafield(namespace: "custom", key: "timezone") { value }
            }
          }
        }`, { query: `handle:${handleFilter}` });

      const products = data?.products?.nodes || [];

      await processProducts({
        products,
        STORE, VERSION, headers,
        defaultTZ: DEFAULT_TZ, force, locationId,
        updated, skipped, debug, wantDebug
      });

      return {
        statusCode: 200,
        body: JSON.stringify({
          message: 'Run OK (handle)',
          filters: { handleFilter, force },
          counts: { updated: updated.length, skipped: skipped.length },
          updated, skipped, debug
        }, null, 2)
      };
    }

    // 3) MASIVO: solo productos con ambos metafields
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
                nodes {
                  id
                  legacyResourceId
                  inventoryItem { id legacyResourceId tracked }
                }
              }
              metafield(namespace: "custom", key: "fecha_disponibilidad") { value }
              metafieldStock: metafield(namespace: "custom", key: "stock_programado") { value }
              metafieldTZ:   metafield(namespace: "custom", key: "timezone") { value }
            }
          }
        }`, { after: cursor });

      const products = data?.products?.nodes || [];
      totalFetched += products.length;

      await processProducts({
        products,
        STORE, VERSION, headers,
        defaultTZ: DEFAULT_TZ, force, locationId,
        updated, skipped, debug, wantDebug
      });

      hasNextPage = data?.products?.pageInfo?.hasNextPage || false;
      cursor      = data?.products?.pageInfo?.endCursor || null;

      await sleep(400); // pacing GraphQL
    }

    return {
      statusCode: 200,
      body: JSON.stringify({
        message: 'Run OK (mass)',
        counts: { fetchedWithMetafields: totalFetched, updated: updated.length, skipped: skipped.length },
        updated, skipped, debug
      }, null, 2)
    };

  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err?.message || String(err) }) };
  }
};

async function processProducts({
  products, STORE, VERSION, headers, defaultTZ, force, locationId,
  updated, skipped, debug, wantDebug
}) {
  for (const p of products) {
    const fecha = p?.metafield?.value ? String(p.metafield.value).slice(0, 10) : null;
    const stock = p?.metafieldStock?.value ? parseInt(p.metafieldStock.value, 10) : 0;
    const productTZ = (p?.metafieldTZ?.value || defaultTZ).trim();
    const todayLocal = ymdInTZ(new Date(), productTZ);

    if (wantDebug) {
      debug.push({ productId: p.id, handle: p.handle, fecha, stock, productTZ, todayLocal });
    }

    if (!force && (!fecha || fecha !== todayLocal)) {
      skipped.push({ productId: p.id, reason: 'date_mismatch_or_empty', fecha, todayLocal, stock });
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

    // Asegurar que el inventory item esté "tracked"
    if (v.inventoryItem && v.inventoryItem.tracked === false) {
      try {
        await putJSON(`${STORE}/admin/api/${VERSION}/inventory_items/${v.inventoryItem.legacyResourceId}.json`, headers, {
          inventory_item: { id: v.inventoryItem.legacyResourceId, tracked: true }
        });
      } catch (e) {
        skipped.push({ productId: p.id, variantId: v.legacyResourceId, reason: 'enable_tracked_failed', error: e.message });
        await sleep(700);
        continue;
      }
    }

    // Setear inventario en la location
    try {
      const r = await fetch(`${STORE}/admin/api/${VERSION}/inventory_levels/set.json`, {
        method: 'POST', headers,
        body: JSON.stringify({
          location_id: locationId,
          inventory_item_id: v.inventoryItem.legacyResourceId,
          available: stock
        })
      });
      const text = await r.text();
      if (!r.ok) throw new Error(`SET inventory -> ${r.status}: ${text}`);

      updated.push({
        productId: p.id,
        handle: p.handle,
        variantId: v.legacyResourceId,
        inventoryItemId: v.inventoryItem.legacyResourceId,
        locationId,
        setTo: stock,
        fechaAplicada: force ? '(FORCED)' : todayLocal
      });
    } catch (e) {
      skipped.push({ productId: p.id, variantId: v.legacyResourceId, reason: 'inventory_set_failed', error: e.message });
    }

    await sleep(300); // pacing REST
  }
}
