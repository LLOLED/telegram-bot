export default {
  async fetch(request, env) {
    if (request.method !== 'POST') return new Response('OK', { status: 200 });
    try {
      const update = await request.json();
      await processUpdate(update, env);
    } catch (e) { console.error(e); }
    return new Response('OK', { status: 200 });
  },
  async scheduled(event, env, ctx) {
    ctx.waitUntil(runScheduledTasks(env));
  }
};

const API = (env) => `https://api.telegram.org/bot${env.BOT_TOKEN}`;
const TTL = 60 * 60 * 24 * 365;

function defaultSettings() {
  return {
    links_locked: false,
    group_locked: false,
    media_lock: { photo: false, video: false, sticker: false, audio: false, mic: false, gif: false, document: false, poll: false },
    warnings_enabled: true,
    max_warnings: 3,
    warn_action: 'kick',
    banned_words: [],
    auto_replies: {},
    welcome_enabled: false,
    welcome_message: 'اهلاً {name} في {group}',
    captcha_enabled: false,
    captcha_type: 'button',
    night_mode: { enabled: false, start: 23, end: 6 },
    anti_channel: false,
    anti_forward: false,
    sub_admins: [],
    default_mute_duration: 3600,
    slow_mode_delay: 0,
    anti_flood: { enabled: false, max_messages: 5, window_seconds: 10, action: 'mute' },
    link_whitelist: [],
    log_chat_id: null,
  };
}

// ─── Settings ─────────────────────────────────────────────────────────────────

async function getSettings(env, chatId) {
  try {
    const d = await env.GROUP_SETTINGS.get('s_' + chatId);
    if (!d) return defaultSettings();
    const saved = JSON.parse(d);
    const def = defaultSettings();
    return {
      ...def,
      ...saved,
      media_lock: { ...def.media_lock, ...(saved.media_lock || {}) },
      night_mode: { ...def.night_mode, ...(saved.night_mode || {}) },
      anti_flood: { ...def.anti_flood, ...(saved.anti_flood || {}) },
      sub_admins: saved.sub_admins || [],
      link_whitelist: saved.link_whitelist || [],
    };
  } catch { return defaultSettings(); }
}

async function saveSettings(env, chatId, s) {
  try {
    await env.GROUP_SETTINGS.put('s_' + chatId, JSON.stringify(s), { expirationTtl: TTL });
  } catch {}
}

// ─── Warnings ─────────────────────────────────────────────────────────────────

async function getWarnings(env, chatId, userId) {
  try {
    const d = await env.USER_DATA.get('w_' + chatId + '_' + userId);
    return d ? parseInt(d) : 0;
  } catch { return 0; }
}

async function setWarnings(env, chatId, userId, count) {
  try {
    await env.USER_DATA.put('w_' + chatId + '_' + userId, count.toString(), { expirationTtl: TTL });
  } catch {}
}

// ─── User Groups ──────────────────────────────────────────────────────────────

async function getUserGroups(env, userId) {
  try {
    const d = await env.USER_DATA.get('groups_' + userId);
    return d ? JSON.parse(d) : {};
  } catch { return {}; }
}

async function addUserGroup(env, userId, chatId, chatTitle) {
  try {
    const groups = await getUserGroups(env, userId);
    groups[chatId.toString()] = chatTitle || chatId.toString();
    await env.USER_DATA.put('groups_' + userId, JSON.stringify(groups), { expirationTtl: TTL });
  } catch {}
}

async function registerAdmins(env, chatId, chatTitle) {
  try {
    const r = await fetch(API(env) + '/getChatAdministrators', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: chatId })
    });
    const d = await r.json();
    if (d.ok && d.result.length > 0) {
      for (const admin of d.result) {
        if (!admin.user.is_bot) {
          await addUserGroup(env, admin.user.id, chatId, chatTitle);
        }
      }
    }
    await addActiveGroup(env, chatId.toString());
  } catch {}
}

// ─── Active Groups (for scheduled tasks) ─────────────────────────────────────

async function getActiveGroups(env) {
  try {
    const d = await env.USER_DATA.get('meta_active_groups');
    return d ? JSON.parse(d) : [];
  } catch { return []; }
}

async function addActiveGroup(env, chatId) {
  try {
    const groups = await getActiveGroups(env);
    if (!groups.includes(chatId.toString())) {
      groups.push(chatId.toString());
      await env.USER_DATA.put('meta_active_groups', JSON.stringify(groups), { expirationTtl: TTL });
    }
  } catch {}
}

// ─── Announcements ────────────────────────────────────────────────────────────

async function getAnnouncements(env, chatId) {
  try {
    const d = await env.USER_DATA.get('ann_' + chatId);
    return d ? JSON.parse(d) : [];
  } catch { return []; }
}

async function saveAnnouncements(env, chatId, list) {
  try {
    await env.USER_DATA.put('ann_' + chatId, JSON.stringify(list), { expirationTtl: TTL });
  } catch {}
}

// ─── Scheduled Announcements ──────────────────────────────────────────────────

async function getScheduledAnns(env, chatId) {
  try {
    const d = await env.USER_DATA.get('sched_' + chatId);
    return d ? JSON.parse(d) : [];
  } catch { return []; }
}

async function saveScheduledAnns(env, chatId, list) {
  try {
    await env.USER_DATA.put('sched_' + chatId, JSON.stringify(list), { expirationTtl: TTL });
  } catch {}
}

async function runScheduledTasks(env) {
  try {
    const groups = await getActiveGroups(env);
    const nowHour = (new Date().getUTCHours() + 3) % 24;
    const todayKey = new Date().toISOString().substring(0, 10);
    for (const chatId of groups) {
      const anns = await getScheduledAnns(env, chatId);
      let changed = false;
      for (const ann of anns) {
        if (!ann.active) continue;
        if (ann.hour !== nowHour) continue;
        if (ann.lastSentDate === todayKey) continue;
        if (ann.photoFileId) {
          await sendPhoto(env, parseInt(chatId), ann.photoFileId, ann.text || '');
        } else {
          await sendMsg(env, parseInt(chatId), ann.text);
        }
        ann.lastSentDate = todayKey;
        if (!ann.repeat) ann.active = false;
        changed = true;
      }
      if (changed) await saveScheduledAnns(env, chatId, anns);
    }
  } catch (e) { console.error('scheduled error', e); }
}

// ─── Pending State ────────────────────────────────────────────────────────────

async function getPending(env, userId) {
  try {
    const d = await env.USER_DATA.get('pend_' + userId);
    return d ? JSON.parse(d) : null;
  } catch { return null; }
}

async function setPending(env, userId, state) {
  try {
    if (state === null) {
      await env.USER_DATA.delete('pend_' + userId);
    } else {
      await env.USER_DATA.put('pend_' + userId, JSON.stringify(state), { expirationTtl: 3600 });
    }
  } catch {}
}

// ─── Banned List ──────────────────────────────────────────────────────────────

async function getBannedList(env, chatId) {
  try {
    const d = await env.USER_DATA.get('blist_' + chatId);
    return d ? JSON.parse(d) : [];
  } catch { return []; }
}

async function saveBannedList(env, chatId, list) {
  try {
    await env.USER_DATA.put('blist_' + chatId, JSON.stringify(list), { expirationTtl: TTL });
  } catch {}
}

async function addToBannedList(env, chatId, userId, firstName) {
  try {
    const list = await getBannedList(env, chatId);
    if (!list.find(u => u.id === userId.toString())) {
      list.push({ id: userId.toString(), name: firstName || userId.toString(), date: new Date().toISOString().substring(0, 10) });
      await saveBannedList(env, chatId, list);
    }
  } catch {}
}

async function removeFromBannedList(env, chatId, userId) {
  try {
    let list = await getBannedList(env, chatId);
    list = list.filter(u => u.id !== userId.toString());
    await saveBannedList(env, chatId, list);
  } catch {}
}

// ─── Flood Detection ──────────────────────────────────────────────────────────

async function checkFlood(env, chatId, userId, settings) {
  if (!settings.anti_flood.enabled) return false;
  try {
    const key = 'flood_' + chatId + '_' + userId;
    const now = Math.floor(Date.now() / 1000);
    const d = await env.USER_DATA.get(key);
    let data = d ? JSON.parse(d) : { count: 0, first: now };
    if (now - data.first > settings.anti_flood.window_seconds) {
      data = { count: 1, first: now };
    } else {
      data.count++;
    }
    await env.USER_DATA.put(key, JSON.stringify(data), { expirationTtl: 120 });
    return data.count > settings.anti_flood.max_messages;
  } catch { return false; }
}

// ─── Action Log ───────────────────────────────────────────────────────────────

async function sendLog(env, settings, text) {
  if (!settings.log_chat_id) return;
  try {
    await sendMsg(env, parseInt(settings.log_chat_id), '📋 سجل الإجراءات\n\n' + text);
  } catch {}
}

// ─── Math Captcha ─────────────────────────────────────────────────────────────

function generateMathQuestion() {
  const a = Math.floor(Math.random() * 9) + 1;
  const b = Math.floor(Math.random() * 9) + 1;
  const correct = a + b;
  const wrongs = new Set();
  while (wrongs.size < 3) {
    const w = Math.floor(Math.random() * 18) + 1;
    if (w !== correct) wrongs.add(w);
  }
  const options = [correct, ...wrongs].sort(() => Math.random() - 0.5);
  return { question: a + ' + ' + b + ' = ?', correct, options };
}

async function setMathCaptchaAnswer(env, chatId, userId, answer) {
  try {
    await env.USER_DATA.put('capmath_' + chatId + '_' + userId, answer.toString(), { expirationTtl: 300 });
  } catch {}
}

async function getMathCaptchaAnswer(env, chatId, userId) {
  try {
    const d = await env.USER_DATA.get('capmath_' + chatId + '_' + userId);
    return d ? parseInt(d) : null;
  } catch { return null; }
}

// ─── Authorization ────────────────────────────────────────────────────────────

async function isAuthorized(env, chatId, userId) {
  const member = await getChatMember(env, chatId, userId);
  if (member && (member.status === 'creator' || member.status === 'administrator')) return true;
  const settings = await getSettings(env, chatId);
  return settings.sub_admins.includes(userId.toString());
}

async function isOwner(env, chatId, userId) {
  const member = await getChatMember(env, chatId, userId);
  return member && member.status === 'creator';
}

// ─── Main Processor ───────────────────────────────────────────────────────────

async function processUpdate(update, env) {
  if (update.message) await handleMessage(update.message, env);
  else if (update.callback_query) await handleCallback(update.callback_query, env);
}

async function handleMessage(msg, env) {
  const chatId = msg.chat.id;
  const userId = msg.from ? msg.from.id : null;
  const text = msg.text || '';
  const chatType = msg.chat.type;
  const msgId = msg.message_id;

  if (!userId) return;

  // ── Private chat ──────────────────────────────────────────────────────────
  if (chatType === 'private') {
    const pending = await getPending(env, userId);

    if (pending) {

      if (pending.step === 'await_banned_word') {
        const gChatId = pending.gChatId;
        if (!await isAuthorized(env, gChatId, userId)) { await setPending(env, userId, null); await sendMsg(env, chatId, 'انتهت صلاحيتك.'); return; }
        const word = text.trim();
        if (word) {
          const settings = await getSettings(env, gChatId);
          if (!settings.banned_words.includes(word)) {
            settings.banned_words.push(word);
            await saveSettings(env, gChatId, settings);
            await setPending(env, userId, null);
            await sendMsg(env, chatId, '✅ تمت إضافة الكلمة المحظورة: "' + word + '"',
              { inline_keyboard: [
                [{ text: '🚫 إدارة الكلمات المحظورة', callback_data: 'words_' + gChatId }],
                [{ text: '🔙 رجوع للقائمة', callback_data: 'menu_' + gChatId }],
              ]}
            );
          } else {
            await setPending(env, userId, null);
            await sendMsg(env, chatId, '⚠️ هذه الكلمة موجودة مسبقاً.', { inline_keyboard: [[{ text: '🔙 رجوع', callback_data: 'words_' + gChatId }]] });
          }
        } else { await sendMsg(env, chatId, 'أرسل الكلمة المراد حظرها:'); }
        return;
      }

      if (pending.step === 'await_welcome_msg') {
        const gChatId = pending.gChatId;
        if (!await isAuthorized(env, gChatId, userId)) { await setPending(env, userId, null); await sendMsg(env, chatId, 'انتهت صلاحيتك.'); return; }
        const newMsg = text.trim();
        if (newMsg) {
          const settings = await getSettings(env, gChatId);
          settings.welcome_message = newMsg;
          await saveSettings(env, gChatId, settings);
          await setPending(env, userId, null);
          await sendMsg(env, chatId, '✅ تم تحديث رسالة الترحيب!\n\n' + esc(newMsg),
            { inline_keyboard: [
              [{ text: '👋 إعدادات الترحيب', callback_data: 'welcome_' + gChatId }],
              [{ text: '🔙 رجوع للقائمة', callback_data: 'menu_' + gChatId }],
            ]}
          );
        } else { await sendMsg(env, chatId, 'أرسل نص رسالة الترحيب:'); }
        return;
      }

      if (pending.step === 'await_night_start') {
        const gChatId = pending.gChatId;
        if (!await isAuthorized(env, gChatId, userId)) { await setPending(env, userId, null); await sendMsg(env, chatId, 'انتهت صلاحيتك.'); return; }
        const hour = parseInt(text.trim());
        if (isNaN(hour) || hour < 0 || hour > 23) { await sendMsg(env, chatId, '❌ أرسل رقم صحيح بين 0 و 23:'); return; }
        await setPending(env, userId, { step: 'await_night_end', gChatId, night_start: hour });
        await sendMsg(env, chatId, '✅ ساعة البداية: ' + hour + ':00\n\nالآن أرسل ساعة الانتهاء (0-23):');
        return;
      }

      if (pending.step === 'await_night_end') {
        const gChatId = pending.gChatId;
        if (!await isAuthorized(env, gChatId, userId)) { await setPending(env, userId, null); await sendMsg(env, chatId, 'انتهت صلاحيتك.'); return; }
        const hour = parseInt(text.trim());
        if (isNaN(hour) || hour < 0 || hour > 23) { await sendMsg(env, chatId, '❌ أرسل رقم صحيح بين 0 و 23:'); return; }
        const settings = await getSettings(env, gChatId);
        settings.night_mode.start = pending.night_start;
        settings.night_mode.end = hour;
        await saveSettings(env, gChatId, settings);
        await setPending(env, userId, null);
        await sendMsg(env, chatId, '✅ تم تحديث وقت الوضع الليلي!\nمن ' + pending.night_start + ':00 حتى ' + hour + ':00',
          { inline_keyboard: [
            [{ text: '🌙 إعدادات الوضع الليلي', callback_data: 'night_' + gChatId }],
            [{ text: '🔙 رجوع للقائمة', callback_data: 'menu_' + gChatId }],
          ]}
        );
        return;
      }

      if (pending.step === 'await_sub_admin_id') {
        const gChatId = pending.gChatId;
        if (!await isOwner(env, gChatId, userId)) { await setPending(env, userId, null); await sendMsg(env, chatId, 'هذه الخاصية للمالك فقط.'); return; }
        const subId = text.trim();
        if (!/^\d+$/.test(subId)) { await sendMsg(env, chatId, '❌ أرسل ID رقمي صحيح:'); return; }
        const settings = await getSettings(env, gChatId);
        if (!settings.sub_admins.includes(subId)) {
          settings.sub_admins.push(subId);
          await saveSettings(env, gChatId, settings);
          await addUserGroup(env, parseInt(subId), gChatId, pending.groupName || gChatId);
        }
        await setPending(env, userId, null);
        await sendMsg(env, chatId, '✅ تمت إضافة المشرف الفرعي!\nID: ' + subId,
          { inline_keyboard: [
            [{ text: '👮 إدارة المشرفين الفرعيين', callback_data: 'subadmins_' + gChatId }],
            [{ text: '🔙 رجوع للقائمة', callback_data: 'menu_' + gChatId }],
          ]}
        );
        return;
      }

      if (pending.step === 'await_ann_content') {
        const gChatId = pending.gChatId;
        if (!await isAuthorized(env, gChatId, userId)) { await setPending(env, userId, null); await sendMsg(env, chatId, 'أنت لست مشرفاً في هذه المجموعة بعد الآن.'); return; }
        let photoFileId = null;
        let annText = msg.caption || msg.text || '';
        if (msg.photo && msg.photo.length > 0) photoFileId = msg.photo[msg.photo.length - 1].file_id;
        if (!annText && !photoFileId) { await sendMsg(env, chatId, 'الرجاء إرسال نص أو صورة.'); return; }
        await setPending(env, userId, { step: 'confirm_ann', gChatId, annText, photoFileId: photoFileId || null, orig_msg_id: pending.orig_msg_id, orig_chat_id: pending.orig_chat_id });
        const previewCaption = '📋 معاينة الإعلان:\n\n' + (annText || '(بدون نص)') + '\n\nهل تريد إرساله وتثبيته في المجموعة؟';
        if (photoFileId) {
          await sendPhoto(env, chatId, photoFileId, previewCaption, { inline_keyboard: [[{ text: '📌 إرسال وتثبيت', callback_data: 'ann_confirm_' + gChatId }], [{ text: '❌ إلغاء', callback_data: 'ann_cancel_' + gChatId }]] });
        } else {
          await sendMsg(env, chatId, previewCaption, { inline_keyboard: [[{ text: '📌 إرسال وتثبيت', callback_data: 'ann_confirm_' + gChatId }], [{ text: '❌ إلغاء', callback_data: 'ann_cancel_' + gChatId }]] });
        }
        return;
      }

      // ── إضافة رد تلقائي - الكلمة ─────────────────────────────────────────
      if (pending.step === 'await_reply_keyword') {
        const gChatId = pending.gChatId;
        if (!await isAuthorized(env, gChatId, userId)) { await setPending(env, userId, null); await sendMsg(env, chatId, 'انتهت صلاحيتك.'); return; }
        const kw = text.trim();
        if (!kw) { await sendMsg(env, chatId, 'أرسل الكلمة المفتاحية:'); return; }
        await setPending(env, userId, { step: 'await_reply_text', gChatId, keyword: kw });
        await sendMsg(env, chatId, '✅ الكلمة: "' + esc(kw) + '"\n\nالآن أرسل نص الرد التلقائي:', { inline_keyboard: [[{ text: '❌ إلغاء', callback_data: 'replies_' + gChatId }]] });
        return;
      }

      // ── إضافة رد تلقائي - النص ───────────────────────────────────────────
      if (pending.step === 'await_reply_text') {
        const gChatId = pending.gChatId;
        if (!await isAuthorized(env, gChatId, userId)) { await setPending(env, userId, null); await sendMsg(env, chatId, 'انتهت صلاحيتك.'); return; }
        const replyText = text.trim();
        if (!replyText) { await sendMsg(env, chatId, 'أرسل نص الرد:'); return; }
        const settings = await getSettings(env, gChatId);
        settings.auto_replies[pending.keyword] = replyText;
        await saveSettings(env, gChatId, settings);
        await setPending(env, userId, null);
        await sendMsg(env, chatId, '✅ تمت إضافة الرد التلقائي!\n\nالكلمة: "' + esc(pending.keyword) + '"\nالرد: ' + esc(replyText),
          { inline_keyboard: [
            [{ text: '🤖 الردود التلقائية', callback_data: 'replies_' + gChatId }],
            [{ text: '🔙 رجوع للقائمة', callback_data: 'menu_' + gChatId }],
          ]}
        );
        return;
      }

      // ── إضافة رابط مسموح ─────────────────────────────────────────────────
      if (pending.step === 'await_whitelist_link') {
        const gChatId = pending.gChatId;
        if (!await isAuthorized(env, gChatId, userId)) { await setPending(env, userId, null); await sendMsg(env, chatId, 'انتهت صلاحيتك.'); return; }
        const link = text.trim().toLowerCase();
        if (!link) { await sendMsg(env, chatId, 'أرسل الرابط أو النطاق المسموح:'); return; }
        const settings = await getSettings(env, gChatId);
        if (!settings.link_whitelist.includes(link)) {
          settings.link_whitelist.push(link);
          await saveSettings(env, gChatId, settings);
        }
        await setPending(env, userId, null);
        await sendMsg(env, chatId, '✅ تمت إضافة الرابط المسموح: ' + esc(link),
          { inline_keyboard: [
            [{ text: '🔗 قائمة الروابط المسموحة', callback_data: 'whitelist_menu_' + gChatId }],
            [{ text: '🔙 رجوع للقائمة', callback_data: 'menu_' + gChatId }],
          ]}
        );
        return;
      }

      // ── تعيين قناة السجل ─────────────────────────────────────────────────
      if (pending.step === 'await_log_chat_id') {
        const gChatId = pending.gChatId;
        if (!await isOwner(env, gChatId, userId)) { await setPending(env, userId, null); await sendMsg(env, chatId, 'هذه الخاصية للمالك فقط.'); return; }
        const logId = text.trim();
        if (logId !== 'نفسي' && !/^-?\d+$/.test(logId)) { await sendMsg(env, chatId, '❌ أرسل ID المحادثة الرقمي أو اكتب "نفسي":'); return; }
        const settings = await getSettings(env, gChatId);
        settings.log_chat_id = logId === 'نفسي' ? userId.toString() : logId;
        await saveSettings(env, gChatId, settings);
        await setPending(env, userId, null);
        await sendMsg(env, chatId, '✅ تم تعيين وجهة السجل بنجاح!',
          { inline_keyboard: [
            [{ text: '📋 إعدادات السجل', callback_data: 'log_menu_' + gChatId }],
            [{ text: '🔙 رجوع للقائمة', callback_data: 'menu_' + gChatId }],
          ]}
        );
        return;
      }

      // ── الإعلانات المجدولة - الوقت ───────────────────────────────────────
      if (pending.step === 'await_sched_hour') {
        const gChatId = pending.gChatId;
        if (!await isAuthorized(env, gChatId, userId)) { await setPending(env, userId, null); await sendMsg(env, chatId, 'انتهت صلاحيتك.'); return; }
        const hour = parseInt(text.trim());
        if (isNaN(hour) || hour < 0 || hour > 23) { await sendMsg(env, chatId, '❌ أرسل رقم بين 0 و 23:'); return; }
        await setPending(env, userId, { ...pending, step: 'await_sched_content', hour });
        await sendMsg(env, chatId, '✅ الوقت: ' + hour + ':00\n\nالآن أرسل نص الإعلان (أو صورة مع نص):', { inline_keyboard: [[{ text: '❌ إلغاء', callback_data: 'sched_menu_' + gChatId }]] });
        return;
      }

      if (pending.step === 'await_sched_content') {
        const gChatId = pending.gChatId;
        if (!await isAuthorized(env, gChatId, userId)) { await setPending(env, userId, null); await sendMsg(env, chatId, 'انتهت صلاحيتك.'); return; }
        let photoFileId = null;
        let annText = msg.caption || msg.text || '';
        if (msg.photo && msg.photo.length > 0) photoFileId = msg.photo[msg.photo.length - 1].file_id;
        if (!annText && !photoFileId) { await sendMsg(env, chatId, 'أرسل نص أو صورة:'); return; }
        const anns = await getScheduledAnns(env, gChatId);
        anns.push({ id: Date.now(), text: annText, photoFileId: photoFileId || null, hour: pending.hour, repeat: pending.repeat || false, active: true, lastSentDate: null });
        await saveScheduledAnns(env, gChatId, anns);
        await setPending(env, userId, null);
        await sendMsg(env, chatId, '✅ تم إضافة الإعلان المجدول!\n\nالوقت: ' + pending.hour + ':00\nالتكرار: ' + (pending.repeat ? 'يومياً' : 'مرة واحدة'),
          { inline_keyboard: [
            [{ text: '🕐 الإعلانات المجدولة', callback_data: 'sched_menu_' + gChatId }],
            [{ text: '🔙 رجوع للقائمة', callback_data: 'menu_' + gChatId }],
          ]}
        );
        return;
      }
    }

    if (text === '/start') {
      await sendMsg(env, chatId,
        'اهلاً بك في بوت إدارة المجموعات\n\nأضف البوت لمجموعتك كمشرف ثم اضغط زر إدارة المجموعات',
        { inline_keyboard: [[{ text: '👥 مجموعاتي', callback_data: 'my_groups' }]] }
      );
    }
    return;
  }

  if (chatType !== 'group' && chatType !== 'supergroup') return;

  const member = await getChatMember(env, chatId, userId);
  const isAdmin = member && (member.status === 'creator' || member.status === 'administrator');

  await registerAdmins(env, chatId, msg.chat.title || '');

  const settings = await getSettings(env, chatId);

  // ── قفل / فتح المجموعة ────────────────────────────────────────────────────
  if (isAdmin && (text === 'قفل' || text === 'فتح')) {
    if (text === 'قفل') {
      settings.group_locked = true;
      await saveSettings(env, chatId, settings);
      await setChatPermissions(env, chatId, false);
      await sendMsg(env, chatId, '🔒 تم قفل المجموعة\n\nلا يمكن لأي عضو الكتابة الآن.\nأرسل "فتح" لفتحها مرة أخرى.');
    } else {
      settings.group_locked = false;
      await saveSettings(env, chatId, settings);
      await applyMediaPermissions(env, chatId, settings);
      await sendMsg(env, chatId, '🔓 تم فتح المجموعة\n\nيمكن للأعضاء الكتابة الآن.');
    }
    return;
  }

  // ── استقبال الأعضاء الجدد ─────────────────────────────────────────────────
  if (msg.new_chat_members) {
    for (const m of msg.new_chat_members) {
      if (m.is_bot) continue;
      if (settings.captcha_enabled) {
        await restrictUser(env, chatId, m.id, false);
        if (settings.captcha_type === 'math') {
          const q = generateMathQuestion();
          await setMathCaptchaAnswer(env, chatId, m.id, q.correct);
          const btns = q.options.map(o => [{ text: o.toString(), callback_data: 'capmath_' + chatId + '_' + m.id + '_' + o }]);
          await sendMsg(env, chatId,
            'مرحباً ' + esc(m.first_name) + '\nأثبت أنك لست روبوتاً!\n\n🔢 كم يساوي ' + q.question,
            { inline_keyboard: btns }
          );
        } else {
          await sendMsg(env, chatId,
            'مرحباً ' + esc(m.first_name) + '\nاضغط الزر خلال 5 دقائق للتحقق',
            { inline_keyboard: [[{ text: '✅ لست روبوت', callback_data: 'cap_' + m.id + '_' + chatId }]] }
          );
        }
      } else if (settings.welcome_enabled) {
        const wmsg = settings.welcome_message.replace('{name}', m.first_name).replace('{group}', msg.chat.title || '');
        await sendMsg(env, chatId, wmsg);
      }
    }
    return;
  }

  // ── قفل المجموعة ─────────────────────────────────────────────────────────
  if (!isAdmin && settings.group_locked) {
    await deleteMsg(env, chatId, msgId);
    return;
  }

  if (!isAdmin) {
    // ── حماية الفلود ────────────────────────────────────────────────────────
    const flooded = await checkFlood(env, chatId, userId, settings);
    if (flooded) {
      await deleteMsg(env, chatId, msgId);
      const action = settings.anti_flood.action;
      const name = esc(msg.from.first_name);
      if (action === 'ban') {
        await banUserAndTrack(env, chatId, userId, msg.from.first_name);
        await sendMsg(env, chatId, '🚫 ' + name + ' تم حظره بسبب الإرسال السريع!');
        await sendLog(env, settings, '🚫 حظر بسبب فلود\nالعضو: ' + name + '\nID: ' + userId);
      } else if (action === 'kick') {
        await kickUser(env, chatId, userId);
        await sendMsg(env, chatId, '👢 ' + name + ' تم طرده بسبب الإرسال السريع!');
        await sendLog(env, settings, '👢 طرد بسبب فلود\nالعضو: ' + name + '\nID: ' + userId);
      } else {
        const dur = settings.default_mute_duration || 3600;
        const until = await restrictUser(env, chatId, userId, false, dur);
        await setMuteData(env, chatId, userId, until);
        await sendMsg(env, chatId, '🔇 ' + name + ' تم كتمه بسبب الإرسال السريع!');
        await sendLog(env, settings, '🔇 كتم بسبب فلود\nالعضو: ' + name + '\nID: ' + userId);
      }
      return;
    }

    if (settings.links_locked && hasLink(text, settings.link_whitelist)) {
      await deleteMsg(env, chatId, msgId);
      const w = await applyWarning(env, chatId, userId, msg.from.first_name, settings);
      await sendMsg(env, chatId, esc(msg.from.first_name) + ' الروابط ممنوعة!\n' + w);
      await sendLog(env, settings, '🔗 حذف رابط\nالعضو: ' + esc(msg.from.first_name) + '\nالرسالة: ' + esc(text.substring(0, 80)));
      return;
    }

    for (const bw of settings.banned_words) {
      if (text.toLowerCase().includes(bw.toLowerCase())) {
        await deleteMsg(env, chatId, msgId);
        const w = await applyWarning(env, chatId, userId, msg.from.first_name, settings);
        await sendMsg(env, chatId, esc(msg.from.first_name) + ' رسالتك تحتوي على كلمة محظورة!\n' + w);
        await sendLog(env, settings, '🚫 كلمة محظورة\nالعضو: ' + esc(msg.from.first_name) + '\nالكلمة: ' + bw);
        return;
      }
    }

    if (settings.anti_forward && (msg.forward_from || msg.forward_from_chat)) {
      await deleteMsg(env, chatId, msgId);
      await sendMsg(env, chatId, esc(msg.from.first_name) + ' التوجيه ممنوع');
      await sendLog(env, settings, '↩️ حذف توجيه\nالعضو: ' + esc(msg.from.first_name));
      return;
    }
    if (settings.anti_channel && msg.sender_chat) {
      await deleteMsg(env, chatId, msgId);
      return;
    }

    if (settings.night_mode.enabled) {
      const h = (new Date().getUTCHours() + 3) % 24;
      const st = settings.night_mode.start;
      const en = settings.night_mode.end;
      const isNight = st > en ? (h >= st || h < en) : (h >= st && h < en);
      if (isNight) {
        await deleteMsg(env, chatId, msgId);
        await sendMsg(env, chatId, esc(msg.from.first_name) + ' المجموعة في الوضع الليلي');
        return;
      }
    }
  }

  for (const [kw, reply] of Object.entries(settings.auto_replies)) {
    if (text.toLowerCase().includes(kw.toLowerCase())) {
      await sendMsg(env, chatId, reply);
      return;
    }
  }

  if (isAdmin && text.startsWith('/')) {
    const parts = text.split(' ');
    const cmd = parts[0].split('@')[0];
    const reply = msg.reply_to_message;

    if (cmd === '/ban' && reply) {
      await banUserAndTrack(env, chatId, reply.from.id, reply.from.first_name);
      await sendMsg(env, chatId, 'تم حظر ' + esc(reply.from.first_name));
      await sendLog(env, settings, '🚫 حظر يدوي\nالعضو: ' + esc(reply.from.first_name) + '\nبواسطة: ' + esc(msg.from.first_name));
    } else if (cmd === '/unban' && reply) {
      await unbanUser(env, chatId, reply.from.id);
      await removeFromBannedList(env, chatId, reply.from.id);
      await sendMsg(env, chatId, 'تم فك حظر ' + esc(reply.from.first_name));
    } else if (cmd === '/kick' && reply) {
      await kickUser(env, chatId, reply.from.id);
      await sendMsg(env, chatId, 'تم طرد ' + esc(reply.from.first_name));
      await sendLog(env, settings, '👢 طرد يدوي\nالعضو: ' + esc(reply.from.first_name) + '\nبواسطة: ' + esc(msg.from.first_name));
    } else if (cmd === '/mute' && reply) {
      const customMins = parseInt(parts[1]);
      const durSecs = customMins ? customMins * 60 : (settings.default_mute_duration || 3600);
      const until = await restrictUser(env, chatId, reply.from.id, false, durSecs);
      await setMuteData(env, chatId, reply.from.id, until);
      await sendLog(env, settings, '🔇 كتم يدوي\nالعضو: ' + esc(reply.from.first_name) + '\nبواسطة: ' + esc(msg.from.first_name));
    } else if (cmd === '/unmute' && reply) {
      await restrictUser(env, chatId, reply.from.id, true);
      await sendMsg(env, chatId, 'تم فك كتم ' + esc(reply.from.first_name));
    } else if (cmd === '/warn' && reply) {
      const result = await applyWarning(env, chatId, reply.from.id, reply.from.first_name, settings);
      await sendMsg(env, chatId, result);
    } else if (cmd === '/resetwarn' && reply) {
      await setWarnings(env, chatId, reply.from.id, 0);
      await sendMsg(env, chatId, 'تم مسح إنذارات ' + esc(reply.from.first_name));
    } else if (cmd === '/addreply') {
      const kw = parts[1];
      const rtext = parts.slice(2).join(' ');
      if (kw && rtext) {
        settings.auto_replies[kw] = rtext;
        await saveSettings(env, chatId, settings);
        await sendMsg(env, chatId, 'تم إضافة الرد التلقائي: ' + kw);
      }
    } else if (cmd === '/removereply') {
      const kw = parts[1];
      delete settings.auto_replies[kw];
      await saveSettings(env, chatId, settings);
      await sendMsg(env, chatId, 'تم حذف الرد: ' + kw);
    }
  }
}

// ─── Callbacks ────────────────────────────────────────────────────────────────

async function handleCallback(cb, env) {
  const data = cb.data;
  const userId = cb.from.id;
  const chatId = cb.message.chat.id;
  const msgId = cb.message.message_id;

  await answerCb(env, cb.id);

  // ── كابتشا زر ─────────────────────────────────────────────────────────────
  if (data.startsWith('cap_')) {
    const parts = data.split('_');
    const targetId = parts[1];
    const targetChat = parts[2];
    if (userId.toString() === targetId) {
      await restrictUser(env, parseInt(targetChat), parseInt(targetId), true);
      await deleteMsg(env, chatId, msgId);
      await sendMsg(env, parseInt(targetChat), 'مرحباً ' + esc(cb.from.first_name) + ' تم التحقق بنجاح ✅');
    }
    return;
  }

  // ── كابتشا حسابية ────────────────────────────────────────────────────────
  if (data.startsWith('capmath_')) {
    const parts = data.replace('capmath_', '').split('_');
    const mChatId = parts[0];
    const mUserId = parts[1];
    const chosen = parseInt(parts[2]);
    if (userId.toString() !== mUserId) return;
    const correctAnswer = await getMathCaptchaAnswer(env, mChatId, userId);
    if (correctAnswer === null) {
      await deleteMsg(env, chatId, msgId);
      await sendMsg(env, parseInt(mChatId), esc(cb.from.first_name) + ' انتهت مهلة التحقق، تم طرده.');
      await kickUser(env, parseInt(mChatId), userId);
      return;
    }
    if (chosen === correctAnswer) {
      await restrictUser(env, parseInt(mChatId), userId, true);
      await deleteMsg(env, chatId, msgId);
      await sendMsg(env, parseInt(mChatId), 'مرحباً ' + esc(cb.from.first_name) + ' تم التحقق بنجاح ✅');
    } else {
      await answerCb(env, cb.id, '❌ إجابة خاطئة! حاول مرة أخرى.', true);
    }
    return;
  }

  // ── عداد الكتم الحي ───────────────────────────────────────────────────────
  if (data.startsWith('check_mute_')) {
    const parts = data.replace('check_mute_', '').split('_');
    const mChatId = parts[0];
    const mUserId = parts[1];
    if (userId.toString() !== mUserId) return;
    const untilTs = await getMuteUntil(env, mChatId, userId);
    const now = Math.floor(Date.now() / 1000);
    if (!untilTs || untilTs <= now) {
      await editMsg(env, chatId, msgId, '✅ انتهى الكتم!\nيمكنك الكتابة في المجموعة الآن.');
    } else {
      const rem = untilTs - now;
      const totalMins = Math.floor(rem / 60);
      const secs = rem % 60;
      const hours = Math.floor(totalMins / 60);
      const mins = totalMins % 60;
      const timeStr = hours > 0
        ? hours + ':' + (mins < 10 ? '0' : '') + mins + ':' + (secs < 10 ? '0' : '') + secs
        : mins + ':' + (secs < 10 ? '0' : '') + secs;
      const endFmt = formatMuteExpiry(rem);
      await editMsg(env, chatId, msgId,
        '🔇 أنت مكتوم في المجموعة\n\n⏱ الوقت المتبقي:\n' + timeStr + '\n\n🕐 ينتهي الكتم الساعة: ' + endFmt.time,
        { inline_keyboard: [[{ text: '🔄 تحديث', callback_data: 'check_mute_' + mChatId + '_' + mUserId }]] }
      );
    }
    return;
  }

  if (cb.message.chat.type !== 'private') return;

  // ── Announcement callbacks ─────────────────────────────────────────────────
  if (data.startsWith('ann_confirm_')) {
    const gChatId = data.replace('ann_confirm_', '');
    const pending = await getPending(env, userId);
    if (!pending || pending.step !== 'confirm_ann') return;
    if (!await isAuthorized(env, gChatId, userId)) { await setPending(env, userId, null); await editMsg(env, chatId, msgId, 'أنت لست مشرفاً في هذه المجموعة بعد الآن.'); return; }
    let sentMsg = null;
    if (pending.photoFileId) {
      sentMsg = await sendPhoto(env, parseInt(gChatId), pending.photoFileId, pending.annText || '');
    } else {
      sentMsg = await sendMsg(env, parseInt(gChatId), pending.annText);
    }
    if (sentMsg && sentMsg.message_id) {
      await pinMessage(env, parseInt(gChatId), sentMsg.message_id);
      const anns = await getAnnouncements(env, gChatId);
      anns.push({ id: sentMsg.message_id, text: (pending.annText || '').substring(0, 80), hasPhoto: !!pending.photoFileId, date: new Date().toISOString() });
      await saveAnnouncements(env, gChatId, anns);
      await setPending(env, userId, null);
      await editMsg(env, chatId, msgId, '✅ تم إرسال الإعلان وتثبيته بنجاح!',
        { inline_keyboard: [
          [{ text: '📌 إعلان جديد', callback_data: 'announce_' + gChatId }],
          [{ text: '🗑️ إدارة الإعلانات', callback_data: 'ann_list_' + gChatId }],
          [{ text: '🔙 رجوع للقائمة', callback_data: 'menu_' + gChatId }],
        ]}
      );
    } else {
      await setPending(env, userId, null);
      await editMsg(env, chatId, msgId, '❌ حدث خطأ أثناء الإرسال.', { inline_keyboard: [[{ text: '🔙 رجوع', callback_data: 'menu_' + gChatId }]] });
    }
    return;
  }

  if (data.startsWith('ann_cancel_')) {
    const gChatId = data.replace('ann_cancel_', '');
    await setPending(env, userId, null);
    await editMsg(env, chatId, msgId, '❌ تم إلغاء الإعلان.', { inline_keyboard: [[{ text: '🔙 رجوع', callback_data: 'menu_' + gChatId }]] });
    return;
  }

  if (data.startsWith('ann_list_')) {
    const gChatId = data.replace('ann_list_', '');
    if (!await isAuthorized(env, gChatId, userId)) return;
    await showAnnList(env, chatId, msgId, gChatId);
    return;
  }

  if (data.startsWith('ann_del_')) {
    const withoutPrefix = data.replace('ann_del_', '');
    const firstUnder = withoutPrefix.indexOf('_');
    const idx = parseInt(withoutPrefix.substring(0, firstUnder));
    const gChatId = withoutPrefix.substring(firstUnder + 1);
    if (!await isAuthorized(env, gChatId, userId)) return;
    const anns = await getAnnouncements(env, gChatId);
    if (idx >= 0 && idx < anns.length) {
      const ann = anns[idx];
      await unpinMessage(env, parseInt(gChatId), ann.id);
      await deleteMsg(env, parseInt(gChatId), ann.id);
      anns.splice(idx, 1);
      await saveAnnouncements(env, gChatId, anns);
    }
    await showAnnList(env, chatId, msgId, gChatId, '✅ تم حذف الإعلان.');
    return;
  }

  if (data.startsWith('announce_')) {
    const gChatId = data.replace('announce_', '');
    if (!await isAuthorized(env, gChatId, userId)) return;
    await setPending(env, userId, { step: 'await_ann_content', gChatId, orig_msg_id: msgId, orig_chat_id: chatId });
    await editMsg(env, chatId, msgId,
      '📌 تثبيت إعلان جديد\n\nأرسل نص الإعلان أو صورة مع نص.\nسيتم عرض معاينة قبل الإرسال.',
      { inline_keyboard: [[{ text: '❌ إلغاء', callback_data: 'ann_cancel_' + gChatId }]] }
    );
    return;
  }

  // ── My Groups ─────────────────────────────────────────────────────────────
  if (data === 'my_groups') {
    const groups = await getUserGroups(env, userId);
    const keys = Object.keys(groups);
    if (keys.length === 0) {
      await editMsg(env, chatId, msgId, 'ما عندك مجموعات مسجلة\nأضف البوت لمجموعتك كمشرف وأرسل أي رسالة فيها', { inline_keyboard: [[{ text: '🔄 تحديث', callback_data: 'my_groups' }]] });
      return;
    }
    const btns = keys.map(id => [{ text: groups[id], callback_data: 'grp_' + id }]);
    btns.push([{ text: '🔄 تحديث', callback_data: 'my_groups' }]);
    await editMsg(env, chatId, msgId, 'مجموعاتك:\nاختر مجموعة للإدارة:', { inline_keyboard: btns });
    return;
  }

  if (data.startsWith('grp_')) {
    const gId = data.replace('grp_', '');
    if (!await isAuthorized(env, gId, userId)) { await editMsg(env, chatId, msgId, 'أنت لست مشرفاً في هذه المجموعة'); return; }
    await showMainMenu(env, chatId, msgId, gId, userId);
    return;
  }

  // ── استخراج gChatId ───────────────────────────────────────────────────────
  const lastUnder = data.lastIndexOf('_');
  const gChatId = lastUnder !== -1 ? data.substring(lastUnder + 1) : '';

  if (!await isAuthorized(env, gChatId, userId)) return;

  const settings = await getSettings(env, gChatId);

  // ── Routing ───────────────────────────────────────────────────────────────
  if (data === 'menu_' + gChatId) { await setPending(env, userId, null); await showMainMenu(env, chatId, msgId, gChatId, userId); return; }
  if (data === 'welcome_' + gChatId) { await setPending(env, userId, null); await showWelcomeMenu(env, chatId, msgId, gChatId, settings); return; }
  if (data === 'protect_' + gChatId) { await setPending(env, userId, null); await showProtectMenu(env, chatId, msgId, gChatId, settings); return; }
  if (data === 'media_' + gChatId) { await setPending(env, userId, null); await showMediaMenu(env, chatId, msgId, gChatId, settings); return; }
  if (data === 'warns_' + gChatId) { await setPending(env, userId, null); await showWarnsMenu(env, chatId, msgId, gChatId, settings); return; }
  if (data === 'night_' + gChatId) { await setPending(env, userId, null); await showNightMenu(env, chatId, msgId, gChatId, settings); return; }
  if (data === 'replies_' + gChatId) { await setPending(env, userId, null); await showRepliesMenu(env, chatId, msgId, gChatId, settings); return; }
  if (data === 'words_' + gChatId) { await setPending(env, userId, null); await showWordsMenu(env, chatId, msgId, gChatId, settings); return; }
  if (data === 'subadmins_' + gChatId) { await setPending(env, userId, null); await showSubAdminsMenu(env, chatId, msgId, gChatId, settings, userId); return; }
  if (data === 'mute_menu_' + gChatId) { await setPending(env, userId, null); await showMuteMenu(env, chatId, msgId, gChatId, settings); return; }
  if (data === 'slow_menu_' + gChatId) { await setPending(env, userId, null); await showSlowMenu(env, chatId, msgId, gChatId, settings); return; }
  if (data === 'flood_menu_' + gChatId) { await setPending(env, userId, null); await showFloodMenu(env, chatId, msgId, gChatId, settings); return; }
  if (data === 'banned_menu_' + gChatId) { await setPending(env, userId, null); await showBannedMenu(env, chatId, msgId, gChatId); return; }
  if (data === 'whitelist_menu_' + gChatId) { await setPending(env, userId, null); await showWhitelistMenu(env, chatId, msgId, gChatId, settings); return; }
  if (data === 'log_menu_' + gChatId) { await setPending(env, userId, null); await showLogMenu(env, chatId, msgId, gChatId, settings, userId); return; }
  if (data === 'sched_menu_' + gChatId) { await setPending(env, userId, null); await showSchedMenu(env, chatId, msgId, gChatId); return; }

  // ── إضافة كلمة محظورة ────────────────────────────────────────────────────
  if (data === 'add_word_' + gChatId) {
    await setPending(env, userId, { step: 'await_banned_word', gChatId });
    await editMsg(env, chatId, msgId, '🚫 إضافة كلمة محظورة\n\nأرسل الكلمة التي تريد حظرها:', { inline_keyboard: [[{ text: '❌ إلغاء', callback_data: 'words_' + gChatId }]] });
    return;
  }

  if (data.startsWith('del_word_')) {
    const withoutPrefix = data.replace('del_word_', '');
    const firstUnder = withoutPrefix.indexOf('_');
    const wordIdx = parseInt(withoutPrefix.substring(0, firstUnder));
    const wGChatId = withoutPrefix.substring(firstUnder + 1);
    const wSettings = await getSettings(env, wGChatId);
    if (!isNaN(wordIdx) && wordIdx >= 0 && wordIdx < wSettings.banned_words.length) {
      wSettings.banned_words.splice(wordIdx, 1);
      await saveSettings(env, wGChatId, wSettings);
    }
    await showWordsMenu(env, chatId, msgId, wGChatId, wSettings);
    return;
  }

  // ── تعديل رسالة الترحيب ──────────────────────────────────────────────────
  if (data === 'edit_welcome_' + gChatId) {
    await setPending(env, userId, { step: 'await_welcome_msg', gChatId });
    await editMsg(env, chatId, msgId, '✏️ تعديل رسالة الترحيب\n\nأرسل نص الرسالة الجديدة:\n\n💡 استخدم {name} لاسم العضو و {group} لاسم المجموعة', { inline_keyboard: [[{ text: '❌ إلغاء', callback_data: 'welcome_' + gChatId }]] });
    return;
  }

  // ── تعديل وقت الوضع الليلي ───────────────────────────────────────────────
  if (data === 'edit_night_time_' + gChatId) {
    await setPending(env, userId, { step: 'await_night_start', gChatId });
    await editMsg(env, chatId, msgId, '🌙 تعديل وقت الوضع الليلي\n\nأرسل ساعة البداية (0-23):', { inline_keyboard: [[{ text: '❌ إلغاء', callback_data: 'night_' + gChatId }]] });
    return;
  }

  // ── إضافة مشرف فرعي ──────────────────────────────────────────────────────
  if (data === 'add_subadmin_' + gChatId) {
    if (!await isOwner(env, gChatId, userId)) { await editMsg(env, chatId, msgId, '❌ هذه الخاصية للمالك فقط.', { inline_keyboard: [[{ text: '🔙 رجوع', callback_data: 'subadmins_' + gChatId }]] }); return; }
    const groups = await getUserGroups(env, userId);
    await setPending(env, userId, { step: 'await_sub_admin_id', gChatId, groupName: groups[gChatId.toString()] || gChatId });
    await editMsg(env, chatId, msgId, '👮 إضافة مشرف فرعي\n\nأرسل الـ ID الرقمي للمستخدم:\n\n💡 يمكن معرفة ID عبر @userinfobot', { inline_keyboard: [[{ text: '❌ إلغاء', callback_data: 'subadmins_' + gChatId }]] });
    return;
  }

  if (data.startsWith('del_subadmin_')) {
    const withoutPrefix = data.replace('del_subadmin_', '');
    const firstUnder = withoutPrefix.indexOf('_');
    const subIdx = parseInt(withoutPrefix.substring(0, firstUnder));
    const saGChatId = withoutPrefix.substring(firstUnder + 1);
    if (!await isOwner(env, saGChatId, userId)) return;
    const saSettings = await getSettings(env, saGChatId);
    if (!isNaN(subIdx) && subIdx >= 0 && subIdx < saSettings.sub_admins.length) {
      saSettings.sub_admins.splice(subIdx, 1);
      await saveSettings(env, saGChatId, saSettings);
    }
    await showSubAdminsMenu(env, chatId, msgId, saGChatId, saSettings, userId);
    return;
  }

  // ── كتم بمدة محددة ────────────────────────────────────────────────────────
  if (data.startsWith('mute_dur_')) {
    const parts = data.replace('mute_dur_', '').split('_');
    const durSecs = parseInt(parts[0]);
    const targetId = parseInt(parts[1]);
    const mGChatId = parts.slice(2).join('_');
    if (!await isAuthorized(env, mGChatId, userId)) return;
    await restrictUser(env, parseInt(mGChatId), targetId, false, durSecs);
    const labels = { 600: '10 دقائق', 1800: '30 دقيقة', 3600: 'ساعة واحدة', 43200: '12 ساعة', 86400: '24 ساعة' };
    await editMsg(env, chatId, msgId, '🔇 تم كتم المستخدم لمدة ' + (labels[durSecs] || durSecs / 60 + ' دقيقة') + ' ✅', { inline_keyboard: [[{ text: '🔙 رجوع', callback_data: 'menu_' + mGChatId }]] });
    return;
  }

  if (data.startsWith('mute_def_')) {
    const withoutPrefix = data.replace('mute_def_', '');
    const firstUnder = withoutPrefix.indexOf('_');
    const durSecs = parseInt(withoutPrefix.substring(0, firstUnder));
    const mGChatId = withoutPrefix.substring(firstUnder + 1);
    if (!await isAuthorized(env, mGChatId, userId)) return;
    const mSettings = await getSettings(env, mGChatId);
    mSettings.default_mute_duration = durSecs;
    await saveSettings(env, mGChatId, mSettings);
    await showMuteMenu(env, chatId, msgId, mGChatId, mSettings);
    return;
  }

  if (data.startsWith('slow_set_')) {
    const withoutPrefix = data.replace('slow_set_', '');
    const firstUnder = withoutPrefix.indexOf('_');
    const delaySecs = parseInt(withoutPrefix.substring(0, firstUnder));
    const sGChatId = withoutPrefix.substring(firstUnder + 1);
    if (!await isAuthorized(env, sGChatId, userId)) return;
    const sSettings = await getSettings(env, sGChatId);
    sSettings.slow_mode_delay = delaySecs;
    await saveSettings(env, sGChatId, sSettings);
    await setChatSlowModeDelay(env, parseInt(sGChatId), delaySecs);
    await showSlowMenu(env, chatId, msgId, sGChatId, sSettings);
    return;
  }

  // ── Flood settings ────────────────────────────────────────────────────────
  if (data.startsWith('flood_max_')) {
    const withoutPrefix = data.replace('flood_max_', '');
    const firstUnder = withoutPrefix.indexOf('_');
    const n = parseInt(withoutPrefix.substring(0, firstUnder));
    const fGChatId = withoutPrefix.substring(firstUnder + 1);
    if (!await isAuthorized(env, fGChatId, userId)) return;
    const fSettings = await getSettings(env, fGChatId);
    fSettings.anti_flood.max_messages = n;
    await saveSettings(env, fGChatId, fSettings);
    await showFloodMenu(env, chatId, msgId, fGChatId, fSettings);
    return;
  }

  if (data.startsWith('flood_win_')) {
    const withoutPrefix = data.replace('flood_win_', '');
    const firstUnder = withoutPrefix.indexOf('_');
    const n = parseInt(withoutPrefix.substring(0, firstUnder));
    const fGChatId = withoutPrefix.substring(firstUnder + 1);
    if (!await isAuthorized(env, fGChatId, userId)) return;
    const fSettings = await getSettings(env, fGChatId);
    fSettings.anti_flood.window_seconds = n;
    await saveSettings(env, fGChatId, fSettings);
    await showFloodMenu(env, chatId, msgId, fGChatId, fSettings);
    return;
  }

  if (data.startsWith('flood_act_')) {
    const withoutPrefix = data.replace('flood_act_', '');
    const firstUnder = withoutPrefix.indexOf('_');
    const act = withoutPrefix.substring(0, firstUnder);
    const fGChatId = withoutPrefix.substring(firstUnder + 1);
    if (!await isAuthorized(env, fGChatId, userId)) return;
    const fSettings = await getSettings(env, fGChatId);
    fSettings.anti_flood.action = act;
    await saveSettings(env, fGChatId, fSettings);
    await showFloodMenu(env, chatId, msgId, fGChatId, fSettings);
    return;
  }

  // ── فك حظر من قائمة المحظورين ────────────────────────────────────────────
  if (data.startsWith('unban_')) {
    const withoutPrefix = data.replace('unban_', '');
    const firstUnder = withoutPrefix.indexOf('_');
    const targetUserId = withoutPrefix.substring(0, firstUnder);
    const bGChatId = withoutPrefix.substring(firstUnder + 1);
    if (!await isAuthorized(env, bGChatId, userId)) return;
    await unbanUser(env, parseInt(bGChatId), parseInt(targetUserId));
    await removeFromBannedList(env, bGChatId, targetUserId);
    await showBannedMenu(env, chatId, msgId, bGChatId, '✅ تم فك الحظر.');
    return;
  }

  // ── إضافة رابط مسموح ─────────────────────────────────────────────────────
  if (data === 'add_whitelist_' + gChatId) {
    await setPending(env, userId, { step: 'await_whitelist_link', gChatId });
    await editMsg(env, chatId, msgId, '🔗 إضافة رابط مسموح\n\nأرسل الرابط أو النطاق (مثل: t.me/mychannel):', { inline_keyboard: [[{ text: '❌ إلغاء', callback_data: 'whitelist_menu_' + gChatId }]] });
    return;
  }

  if (data.startsWith('del_whitelist_')) {
    const withoutPrefix = data.replace('del_whitelist_', '');
    const firstUnder = withoutPrefix.indexOf('_');
    const wIdx = parseInt(withoutPrefix.substring(0, firstUnder));
    const wGChatId = withoutPrefix.substring(firstUnder + 1);
    if (!await isAuthorized(env, wGChatId, userId)) return;
    const wSettings = await getSettings(env, wGChatId);
    if (!isNaN(wIdx) && wIdx >= 0 && wIdx < wSettings.link_whitelist.length) {
      wSettings.link_whitelist.splice(wIdx, 1);
      await saveSettings(env, wGChatId, wSettings);
    }
    await showWhitelistMenu(env, chatId, msgId, wGChatId, wSettings);
    return;
  }

  // ── سجل الإجراءات ────────────────────────────────────────────────────────
  if (data === 'set_log_' + gChatId) {
    if (!await isOwner(env, gChatId, userId)) { await editMsg(env, chatId, msgId, '❌ هذه الخاصية للمالك فقط.', { inline_keyboard: [[{ text: '🔙 رجوع', callback_data: 'log_menu_' + gChatId }]] }); return; }
    await setPending(env, userId, { step: 'await_log_chat_id', gChatId });
    await editMsg(env, chatId, msgId, '📋 تعيين وجهة السجل\n\nأرسل ID المحادثة التي تريد إرسال السجل إليها\nأو اكتب "نفسي" لاستقبال السجل في الخاص', { inline_keyboard: [[{ text: '❌ إلغاء', callback_data: 'log_menu_' + gChatId }]] });
    return;
  }

  if (data === 'clear_log_' + gChatId) {
    if (!await isOwner(env, gChatId, userId)) return;
    settings.log_chat_id = null;
    await saveSettings(env, gChatId, settings);
    await showLogMenu(env, chatId, msgId, gChatId, settings, userId);
    return;
  }

  // ── الردود التلقائية - إضافة ─────────────────────────────────────────────
  if (data === 'add_reply_' + gChatId) {
    await setPending(env, userId, { step: 'await_reply_keyword', gChatId });
    await editMsg(env, chatId, msgId, '🤖 إضافة رد تلقائي\n\nأرسل الكلمة المفتاحية التي سيتم الرد عليها:', { inline_keyboard: [[{ text: '❌ إلغاء', callback_data: 'replies_' + gChatId }]] });
    return;
  }

  if (data.startsWith('del_reply_')) {
    const withoutPrefix = data.replace('del_reply_', '');
    const firstUnder = withoutPrefix.indexOf('_');
    const rIdx = parseInt(withoutPrefix.substring(0, firstUnder));
    const rGChatId = withoutPrefix.substring(firstUnder + 1);
    if (!await isAuthorized(env, rGChatId, userId)) return;
    const rSettings = await getSettings(env, rGChatId);
    const keys = Object.keys(rSettings.auto_replies);
    if (rIdx >= 0 && rIdx < keys.length) {
      delete rSettings.auto_replies[keys[rIdx]];
      await saveSettings(env, rGChatId, rSettings);
    }
    await showRepliesMenu(env, chatId, msgId, rGChatId, rSettings);
    return;
  }

  // ── الإعلانات المجدولة ────────────────────────────────────────────────────
  if (data === 'add_sched_once_' + gChatId) {
    await setPending(env, userId, { step: 'await_sched_hour', gChatId, repeat: false });
    await editMsg(env, chatId, msgId, '🕐 إعلان مجدول (مرة واحدة)\n\nأرسل الساعة التي تريد الإرسال فيها (0-23):', { inline_keyboard: [[{ text: '❌ إلغاء', callback_data: 'sched_menu_' + gChatId }]] });
    return;
  }

  if (data === 'add_sched_daily_' + gChatId) {
    await setPending(env, userId, { step: 'await_sched_hour', gChatId, repeat: true });
    await editMsg(env, chatId, msgId, '🔁 إعلان يومي\n\nأرسل الساعة التي تريد الإرسال فيها يومياً (0-23):', { inline_keyboard: [[{ text: '❌ إلغاء', callback_data: 'sched_menu_' + gChatId }]] });
    return;
  }

  if (data.startsWith('del_sched_')) {
    const withoutPrefix = data.replace('del_sched_', '');
    const firstUnder = withoutPrefix.indexOf('_');
    const sIdx = parseInt(withoutPrefix.substring(0, firstUnder));
    const sGChatId = withoutPrefix.substring(firstUnder + 1);
    if (!await isAuthorized(env, sGChatId, userId)) return;
    const anns = await getScheduledAnns(env, sGChatId);
    if (sIdx >= 0 && sIdx < anns.length) { anns.splice(sIdx, 1); await saveScheduledAnns(env, sGChatId, anns); }
    await showSchedMenu(env, chatId, msgId, sGChatId, '✅ تم حذف الإعلان المجدول.');
    return;
  }

  // ── Toggles ───────────────────────────────────────────────────────────────
  const toggles = {
    ['twelcome_' + gChatId]: () => { settings.welcome_enabled = !settings.welcome_enabled; return () => showWelcomeMenu(env, chatId, msgId, gChatId, settings); },
    ['tcaptcha_' + gChatId]: () => { settings.captcha_enabled = !settings.captcha_enabled; return () => showWelcomeMenu(env, chatId, msgId, gChatId, settings); },
    ['tcaptcha_type_' + gChatId]: () => { settings.captcha_type = settings.captcha_type === 'math' ? 'button' : 'math'; return () => showWelcomeMenu(env, chatId, msgId, gChatId, settings); },
    ['tlinks_' + gChatId]: () => { settings.links_locked = !settings.links_locked; return () => showProtectMenu(env, chatId, msgId, gChatId, settings); },
    ['tforward_' + gChatId]: () => { settings.anti_forward = !settings.anti_forward; return () => showProtectMenu(env, chatId, msgId, gChatId, settings); },
    ['tchannel_' + gChatId]: () => { settings.anti_channel = !settings.anti_channel; return () => showProtectMenu(env, chatId, msgId, gChatId, settings); },
    ['twarns_' + gChatId]: () => { settings.warnings_enabled = !settings.warnings_enabled; return () => showWarnsMenu(env, chatId, msgId, gChatId, settings); },
    ['tnight_' + gChatId]: () => { settings.night_mode.enabled = !settings.night_mode.enabled; return () => showNightMenu(env, chatId, msgId, gChatId, settings); },
    ['tflood_' + gChatId]: () => { settings.anti_flood.enabled = !settings.anti_flood.enabled; return () => showFloodMenu(env, chatId, msgId, gChatId, settings); },
    ['tphoto_' + gChatId]: () => { settings.media_lock.photo = !settings.media_lock.photo; return async () => { await applyMediaPermissions(env, parseInt(gChatId), settings); await showMediaMenu(env, chatId, msgId, gChatId, settings); }; },
    ['tvideo_' + gChatId]: () => { settings.media_lock.video = !settings.media_lock.video; return async () => { await applyMediaPermissions(env, parseInt(gChatId), settings); await showMediaMenu(env, chatId, msgId, gChatId, settings); }; },
    ['tsticker_' + gChatId]: () => { settings.media_lock.sticker = !settings.media_lock.sticker; return async () => { await applyMediaPermissions(env, parseInt(gChatId), settings); await showMediaMenu(env, chatId, msgId, gChatId, settings); }; },
    ['taudio_' + gChatId]: () => { settings.media_lock.audio = !settings.media_lock.audio; return async () => { await applyMediaPermissions(env, parseInt(gChatId), settings); await showMediaMenu(env, chatId, msgId, gChatId, settings); }; },
    ['tmic_' + gChatId]: () => { settings.media_lock.mic = !settings.media_lock.mic; return async () => { await applyMediaPermissions(env, parseInt(gChatId), settings); await showMediaMenu(env, chatId, msgId, gChatId, settings); }; },
    ['tgif_' + gChatId]: () => { settings.media_lock.gif = !settings.media_lock.gif; return async () => { await applyMediaPermissions(env, parseInt(gChatId), settings); await showMediaMenu(env, chatId, msgId, gChatId, settings); }; },
    ['tdoc_' + gChatId]: () => { settings.media_lock.document = !settings.media_lock.document; return async () => { await applyMediaPermissions(env, parseInt(gChatId), settings); await showMediaMenu(env, chatId, msgId, gChatId, settings); }; },
    ['tpoll_' + gChatId]: () => { settings.media_lock.poll = !settings.media_lock.poll; return async () => { await applyMediaPermissions(env, parseInt(gChatId), settings); await showMediaMenu(env, chatId, msgId, gChatId, settings); }; },
  };

  if (data.startsWith('wmax_')) {
    const parts2 = data.split('_');
    settings.max_warnings = parseInt(parts2[1]);
    await saveSettings(env, gChatId, settings);
    await showWarnsMenu(env, chatId, msgId, gChatId, settings);
    return;
  }

  if (data.startsWith('wact_')) {
    settings.warn_action = data.split('_')[1];
    await saveSettings(env, gChatId, settings);
    await showWarnsMenu(env, chatId, msgId, gChatId, settings);
    return;
  }

  if (toggles[data]) {
    const refresh = toggles[data]();
    await saveSettings(env, gChatId, settings);
    await refresh();
  }
}

// ─── Menu Renderers ───────────────────────────────────────────────────────────

async function showMainMenu(env, chatId, msgId, gChatId, userId) {
  const groups = await getUserGroups(env, userId);
  const groupName = groups[gChatId.toString()] || gChatId;
  await editMsg(env, chatId, msgId,
    '⚙️ إدارة: ' + esc(groupName) + '\nاختر القسم:',
    { inline_keyboard: [
      [{ text: '👋 الترحيب والكابتشا', callback_data: 'welcome_' + gChatId }],
      [{ text: '🤖 الردود التلقائية', callback_data: 'replies_' + gChatId }],
      [{ text: '🛡️ الحماية', callback_data: 'protect_' + gChatId }],
      [{ text: '🎬 الوسائط', callback_data: 'media_' + gChatId }],
      [{ text: '⚠️ الإنذارات والعقوبات', callback_data: 'warns_' + gChatId }],
      [{ text: '🌙 الوضع الليلي', callback_data: 'night_' + gChatId }],
      [{ text: '🚫 الكلمات المحظورة', callback_data: 'words_' + gChatId }],
      [{ text: '🔇 إعدادات الكتم', callback_data: 'mute_menu_' + gChatId }],
      [{ text: '⏳ وضع الكتمان المؤقت', callback_data: 'slow_menu_' + gChatId }],
      [{ text: '⚡ الحماية من الفلود', callback_data: 'flood_menu_' + gChatId }],
      [{ text: '🚷 قائمة المحظورين', callback_data: 'banned_menu_' + gChatId }],
      [{ text: '🔗 الروابط المسموحة', callback_data: 'whitelist_menu_' + gChatId }],
      [{ text: '📋 سجل الإجراءات', callback_data: 'log_menu_' + gChatId }],
      [{ text: '🕐 الإعلانات المجدولة', callback_data: 'sched_menu_' + gChatId }],
      [{ text: '👮 المشرفون الفرعيون', callback_data: 'subadmins_' + gChatId }],
      [{ text: '📌 تثبيت إعلان', callback_data: 'announce_' + gChatId }],
      [{ text: '🔙 رجوع لمجموعاتي', callback_data: 'my_groups' }],
    ]}
  );
}

async function showAnnList(env, chatId, msgId, gChatId, notice) {
  const anns = await getAnnouncements(env, gChatId);
  let bodyText = (notice ? notice + '\n\n' : '') + '📌 الإعلانات المثبتة\n\n';
  const btns = [];
  if (anns.length === 0) { bodyText += 'لا توجد إعلانات.'; } else {
    anns.forEach((a, i) => {
      const preview = (a.text || '(بدون نص)').substring(0, 40);
      bodyText += (i + 1) + '. ' + (a.hasPhoto ? '🖼️' : '📝') + ' ' + preview + '\n';
      btns.push([{ text: '🗑️ حذف ' + (i + 1), callback_data: 'ann_del_' + i + '_' + gChatId }]);
    });
  }
  btns.push([{ text: '📌 إضافة إعلان', callback_data: 'announce_' + gChatId }]);
  btns.push([{ text: '🔙 رجوع', callback_data: 'menu_' + gChatId }]);
  await editMsg(env, chatId, msgId, bodyText, { inline_keyboard: btns });
}

function on(v) { return v ? '✅' : '❌'; }

async function showWelcomeMenu(env, chatId, msgId, gChatId, s) {
  const captchaTypeLabel = s.captcha_type === 'math' ? '🔢 حسابية' : '🔘 زر';
  await editMsg(env, chatId, msgId,
    '👋 الترحيب والكابتشا\n\nالترحيب: ' + on(s.welcome_enabled) + '\nالكابتشا: ' + on(s.captcha_enabled) + '\nnوع الكابتشا: ' + captchaTypeLabel + '\n\nرسالة الترحيب:\n' + esc(s.welcome_message),
    { inline_keyboard: [
      [{ text: on(s.welcome_enabled) + ' الترحيب', callback_data: 'twelcome_' + gChatId }, { text: on(s.captcha_enabled) + ' الكابتشا', callback_data: 'tcaptcha_' + gChatId }],
      [{ text: '🔄 نوع الكابتشا: ' + captchaTypeLabel, callback_data: 'tcaptcha_type_' + gChatId }],
      [{ text: '✏️ تعديل رسالة الترحيب', callback_data: 'edit_welcome_' + gChatId }],
      [{ text: '🔙 رجوع', callback_data: 'menu_' + gChatId }],
    ]}
  );
}

async function showProtectMenu(env, chatId, msgId, gChatId, s) {
  await editMsg(env, chatId, msgId,
    '🛡️ الحماية',
    { inline_keyboard: [
      [{ text: on(s.links_locked) + ' قفل الروابط', callback_data: 'tlinks_' + gChatId }],
      [{ text: on(s.anti_forward) + ' منع التوجيه', callback_data: 'tforward_' + gChatId }, { text: on(s.anti_channel) + ' منع القنوات', callback_data: 'tchannel_' + gChatId }],
      [{ text: '🔗 الروابط المسموحة', callback_data: 'whitelist_menu_' + gChatId }],
      [{ text: '🔙 رجوع', callback_data: 'menu_' + gChatId }],
    ]}
  );
}

async function showMediaMenu(env, chatId, msgId, gChatId, s) {
  await editMsg(env, chatId, msgId,
    '🎬 قفل الوسائط',
    { inline_keyboard: [
      [{ text: on(s.media_lock.photo) + ' صور', callback_data: 'tphoto_' + gChatId }, { text: on(s.media_lock.video) + ' فيديو', callback_data: 'tvideo_' + gChatId }],
      [{ text: on(s.media_lock.sticker) + ' ملصقات', callback_data: 'tsticker_' + gChatId }, { text: on(s.media_lock.audio) + ' صوت', callback_data: 'taudio_' + gChatId }],
      [{ text: on(s.media_lock.mic) + ' مايك 🎙', callback_data: 'tmic_' + gChatId }, { text: on(s.media_lock.gif) + ' GIF', callback_data: 'tgif_' + gChatId }],
      [{ text: on(s.media_lock.document) + ' ملفات', callback_data: 'tdoc_' + gChatId }, { text: on(s.media_lock.poll) + ' استطلاع 📊', callback_data: 'tpoll_' + gChatId }],
      [{ text: '🔙 رجوع', callback_data: 'menu_' + gChatId }],
    ]}
  );
}

async function showWarnsMenu(env, chatId, msgId, gChatId, s) {
  const acts = { kick: 'طرد', ban: 'حظر', mute: 'كتم' };
  await editMsg(env, chatId, msgId,
    '⚠️ الإنذارات\nالحد الأقصى: ' + s.max_warnings + '\nالعقوبة: ' + acts[s.warn_action],
    { inline_keyboard: [
      [{ text: on(s.warnings_enabled) + ' الإنذارات', callback_data: 'twarns_' + gChatId }],
      [
        { text: s.max_warnings === 2 ? '◀ 2 ▶' : '2', callback_data: 'wmax_2_' + gChatId },
        { text: s.max_warnings === 3 ? '◀ 3 ▶' : '3', callback_data: 'wmax_3_' + gChatId },
        { text: s.max_warnings === 5 ? '◀ 5 ▶' : '5', callback_data: 'wmax_5_' + gChatId },
        { text: s.max_warnings === 10 ? '◀ 10 ▶' : '10', callback_data: 'wmax_10_' + gChatId },
      ],
      [
        { text: s.warn_action === 'kick' ? '◀ طرد ▶' : 'طرد', callback_data: 'wact_kick_' + gChatId },
        { text: s.warn_action === 'ban' ? '◀ حظر ▶' : 'حظر', callback_data: 'wact_ban_' + gChatId },
        { text: s.warn_action === 'mute' ? '◀ كتم ▶' : 'كتم', callback_data: 'wact_mute_' + gChatId },
      ],
      [{ text: '🔙 رجوع', callback_data: 'menu_' + gChatId }],
    ]}
  );
}

async function showNightMenu(env, chatId, msgId, gChatId, s) {
  await editMsg(env, chatId, msgId,
    '🌙 الوضع الليلي\nالحالة: ' + on(s.night_mode.enabled) + '\nمن الساعة ' + s.night_mode.start + ':00 حتى ' + s.night_mode.end + ':00',
    { inline_keyboard: [
      [{ text: on(s.night_mode.enabled) + ' تفعيل الوضع الليلي', callback_data: 'tnight_' + gChatId }],
      [{ text: '⏰ تعديل الوقت', callback_data: 'edit_night_time_' + gChatId }],
      [{ text: '🔙 رجوع', callback_data: 'menu_' + gChatId }],
    ]}
  );
}

async function showRepliesMenu(env, chatId, msgId, gChatId, s) {
  const entries = Object.entries(s.auto_replies);
  let bodyText = '🤖 الردود التلقائية\n\n';
  const btns = [];
  if (entries.length === 0) {
    bodyText += 'لا توجد ردود تلقائية حتى الآن.';
  } else {
    entries.forEach(([k, v], i) => {
      bodyText += (i + 1) + '. "' + k + '" ← ' + v.substring(0, 30) + '\n';
      btns.push([{ text: '🗑️ حذف: ' + k, callback_data: 'del_reply_' + i + '_' + gChatId }]);
    });
  }
  btns.push([{ text: '➕ إضافة رد تلقائي', callback_data: 'add_reply_' + gChatId }]);
  btns.push([{ text: '🔙 رجوع', callback_data: 'menu_' + gChatId }]);
  await editMsg(env, chatId, msgId, bodyText, { inline_keyboard: btns });
}

async function showWordsMenu(env, chatId, msgId, gChatId, s) {
  const btns = [];
  if (s.banned_words.length > 0) {
    s.banned_words.forEach((word, idx) => btns.push([{ text: '🗑️ حذف: ' + word, callback_data: 'del_word_' + idx + '_' + gChatId }]));
  }
  const wordsList = s.banned_words.length > 0 ? s.banned_words.map((w, i) => (i + 1) + '. ' + w).join('\n') : 'لا توجد كلمات محظورة';
  btns.push([{ text: '➕ إضافة كلمة محظورة', callback_data: 'add_word_' + gChatId }]);
  btns.push([{ text: '🔙 رجوع', callback_data: 'menu_' + gChatId }]);
  await editMsg(env, chatId, msgId, '🚫 الكلمات المحظورة\n\n' + wordsList, { inline_keyboard: btns });
}

async function showMuteMenu(env, chatId, msgId, gChatId, s) {
  const cur = s.default_mute_duration || 3600;
  const labels = { 600: '10 دقائق', 1800: '30 دقيقة', 3600: 'ساعة واحدة', 43200: '12 ساعة', 86400: '24 ساعة' };
  const sel = (v) => cur === v ? '◀ ' + labels[v] + ' ▶' : labels[v];
  await editMsg(env, chatId, msgId,
    '🔇 إعدادات الكتم\n\nالمدة الافتراضية: ' + (labels[cur] || cur / 60 + ' دقيقة') + '\n\nيمكنك كتم بمدة مخصصة: /mute 45',
    { inline_keyboard: [
      [{ text: sel(600), callback_data: 'mute_def_600_' + gChatId }, { text: sel(1800), callback_data: 'mute_def_1800_' + gChatId }],
      [{ text: sel(3600), callback_data: 'mute_def_3600_' + gChatId }, { text: sel(43200), callback_data: 'mute_def_43200_' + gChatId }],
      [{ text: sel(86400), callback_data: 'mute_def_86400_' + gChatId }],
      [{ text: '🔙 رجوع', callback_data: 'menu_' + gChatId }],
    ]}
  );
}

async function showSlowMenu(env, chatId, msgId, gChatId, s) {
  const cur = s.slow_mode_delay || 0;
  const opts = [{ label: '❌ إيقاف', val: 0 }, { label: '10 ثوانٍ', val: 10 }, { label: '30 ثانية', val: 30 }, { label: 'دقيقة', val: 60 }, { label: '5 دقائق', val: 300 }, { label: '10 دقائق', val: 600 }, { label: 'ساعة', val: 3600 }];
  const sel = (v, l) => cur === v ? '◀ ' + l + ' ▶' : l;
  const rows = [];
  for (let i = 0; i < opts.length; i += 2) {
    const row = [{ text: sel(opts[i].val, opts[i].label), callback_data: 'slow_set_' + opts[i].val + '_' + gChatId }];
    if (opts[i + 1]) row.push({ text: sel(opts[i + 1].val, opts[i + 1].label), callback_data: 'slow_set_' + opts[i + 1].val + '_' + gChatId });
    rows.push(row);
  }
  rows.push([{ text: '🔙 رجوع', callback_data: 'menu_' + gChatId }]);
  const statusLabel = cur === 0 ? '❌ متوقف' : (opts.find(o => o.val === cur)?.label || cur + ' ثانية');
  await editMsg(env, chatId, msgId, '⏳ وضع الكتمان المؤقت\n\nالحالة: ' + statusLabel, { inline_keyboard: rows });
}

async function showFloodMenu(env, chatId, msgId, gChatId, s) {
  const f = s.anti_flood;
  const maxOpts = [3, 5, 10, 15, 20];
  const winOpts = [5, 10, 20, 30, 60];
  const acts = { mute: 'كتم', kick: 'طرد', ban: 'حظر' };
  await editMsg(env, chatId, msgId,
    '⚡ الحماية من الفلود\n\nالحالة: ' + on(f.enabled) + '\nالحد الأقصى للرسائل: ' + f.max_messages + '\nخلال: ' + f.window_seconds + ' ثانية\nالإجراء: ' + acts[f.action],
    { inline_keyboard: [
      [{ text: on(f.enabled) + ' تفعيل الحماية', callback_data: 'tflood_' + gChatId }],
      [
        ...maxOpts.map(n => ({ text: f.max_messages === n ? '◀' + n + '▶' : n.toString(), callback_data: 'flood_max_' + n + '_' + gChatId }))
      ],
      [
        ...winOpts.map(n => ({ text: f.window_seconds === n ? '◀' + n + 'ث▶' : n + 'ث', callback_data: 'flood_win_' + n + '_' + gChatId }))
      ],
      [
        { text: f.action === 'mute' ? '◀ كتم ▶' : 'كتم', callback_data: 'flood_act_mute_' + gChatId },
        { text: f.action === 'kick' ? '◀ طرد ▶' : 'طرد', callback_data: 'flood_act_kick_' + gChatId },
        { text: f.action === 'ban' ? '◀ حظر ▶' : 'حظر', callback_data: 'flood_act_ban_' + gChatId },
      ],
      [{ text: '🔙 رجوع', callback_data: 'menu_' + gChatId }],
    ]}
  );
}

async function showBannedMenu(env, chatId, msgId, gChatId, notice) {
  const list = await getBannedList(env, gChatId);
  let bodyText = (notice ? notice + '\n\n' : '') + '🚷 قائمة المحظورين\n\n';
  const btns = [];
  if (list.length === 0) {
    bodyText += 'لا يوجد أعضاء محظورون مسجلون.';
  } else {
    list.forEach((u, i) => {
      bodyText += (i + 1) + '. ' + esc(u.name) + ' (ID: ' + u.id + ') - ' + u.date + '\n';
      btns.push([{ text: '🔓 فك حظر ' + esc(u.name), callback_data: 'unban_' + u.id + '_' + gChatId }]);
    });
  }
  btns.push([{ text: '🔙 رجوع', callback_data: 'menu_' + gChatId }]);
  await editMsg(env, chatId, msgId, bodyText, { inline_keyboard: btns });
}

async function showWhitelistMenu(env, chatId, msgId, gChatId, s) {
  let bodyText = '🔗 الروابط المسموحة\n\nعند تفعيل قفل الروابط، هذه الروابط مسموح بها:\n\n';
  const btns = [];
  if (s.link_whitelist.length === 0) {
    bodyText += 'لا توجد روابط مسموحة.';
  } else {
    s.link_whitelist.forEach((link, i) => {
      bodyText += (i + 1) + '. ' + link + '\n';
      btns.push([{ text: '🗑️ حذف: ' + link, callback_data: 'del_whitelist_' + i + '_' + gChatId }]);
    });
  }
  btns.push([{ text: '➕ إضافة رابط مسموح', callback_data: 'add_whitelist_' + gChatId }]);
  btns.push([{ text: '🔙 رجوع', callback_data: 'protect_' + gChatId }]);
  await editMsg(env, chatId, msgId, bodyText, { inline_keyboard: btns });
}

async function showLogMenu(env, chatId, msgId, gChatId, s, userId) {
  const ownerCheck = await isOwner(env, gChatId, userId);
  const statusText = s.log_chat_id ? '✅ مفعّل\nالوجهة: ' + s.log_chat_id : '❌ غير مفعّل';
  const btns = [];
  if (ownerCheck) {
    btns.push([{ text: '⚙️ تعيين وجهة السجل', callback_data: 'set_log_' + gChatId }]);
    if (s.log_chat_id) btns.push([{ text: '🗑️ إلغاء السجل', callback_data: 'clear_log_' + gChatId }]);
  } else {
    btns.push([{ text: 'ℹ️ هذا الإعداد للمالك فقط', callback_data: 'menu_' + gChatId }]);
  }
  btns.push([{ text: '🔙 رجوع', callback_data: 'menu_' + gChatId }]);
  await editMsg(env, chatId, msgId,
    '📋 سجل الإجراءات\n\nيرسل البوت إشعاراً عند كل إجراء (حذف، حظر، كتم، إلخ)\n\nالحالة: ' + statusText,
    { inline_keyboard: btns }
  );
}

async function showSchedMenu(env, chatId, msgId, gChatId, notice) {
  const anns = await getScheduledAnns(env, gChatId);
  let bodyText = (notice ? notice + '\n\n' : '') + '🕐 الإعلانات المجدولة\n\n';
  const btns = [];
  if (anns.length === 0) {
    bodyText += 'لا توجد إعلانات مجدولة.';
  } else {
    anns.forEach((a, i) => {
      const preview = (a.text || '(صورة)').substring(0, 30);
      const repeat = a.repeat ? '🔁 يومي' : '1️⃣ مرة';
      const status = a.active ? '✅' : '⏸';
      bodyText += (i + 1) + '. ' + status + ' ' + repeat + ' | ' + a.hour + ':00 - ' + preview + '\n';
      btns.push([{ text: '🗑️ حذف ' + (i + 1), callback_data: 'del_sched_' + i + '_' + gChatId }]);
    });
  }
  btns.push([{ text: '➕ إضافة مرة واحدة', callback_data: 'add_sched_once_' + gChatId }]);
  btns.push([{ text: '🔁 إضافة يومي', callback_data: 'add_sched_daily_' + gChatId }]);
  btns.push([{ text: '🔙 رجوع', callback_data: 'menu_' + gChatId }]);
  await editMsg(env, chatId, msgId, bodyText, { inline_keyboard: btns });
}

async function showSubAdminsMenu(env, chatId, msgId, gChatId, s, userId) {
  const ownerCheck = await isOwner(env, gChatId, userId);
  const btns = [];
  let bodyText = '👮 المشرفون الفرعيون\n\n';
  if (s.sub_admins.length === 0) {
    bodyText += 'لا يوجد مشرفون فرعيون حالياً.';
  } else {
    s.sub_admins.forEach((id, idx) => {
      bodyText += (idx + 1) + '. ID: ' + id + '\n';
      if (ownerCheck) btns.push([{ text: '🗑️ حذف المشرف ' + (idx + 1), callback_data: 'del_subadmin_' + idx + '_' + gChatId }]);
    });
  }
  if (ownerCheck) {
    bodyText += '\n💡 المشرف الفرعي يدير إعدادات البوت فقط';
    btns.push([{ text: '➕ إضافة مشرف فرعي', callback_data: 'add_subadmin_' + gChatId }]);
  } else {
    bodyText += '\n⚠️ إضافة وحذف المشرفين للمالك فقط';
  }
  btns.push([{ text: '🔙 رجوع', callback_data: 'menu_' + gChatId }]);
  await editMsg(env, chatId, msgId, bodyText, { inline_keyboard: btns });
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

async function applyWarning(env, chatId, userId, firstName, settings) {
  if (!settings.warnings_enabled) return '';
  let warns = await getWarnings(env, chatId, userId);
  warns++;
  await setWarnings(env, chatId, userId, warns);
  if (warns >= settings.max_warnings) {
    await setWarnings(env, chatId, userId, 0);
    if (settings.warn_action === 'ban') { await banUserAndTrack(env, chatId, userId, firstName); return 'تم حظر ' + esc(firstName) + ' بعد ' + settings.max_warnings + ' إنذارات'; }
    if (settings.warn_action === 'kick') { await kickUser(env, chatId, userId); return 'تم طرد ' + esc(firstName) + ' بعد ' + settings.max_warnings + ' إنذارات'; }
    if (settings.warn_action === 'mute') {
      const until = await restrictUser(env, chatId, userId, false, settings.default_mute_duration || 3600);
      await setMuteData(env, chatId, userId, until);
      return '';
    }
  }
  return '⚠️ إنذار ' + warns + '/' + settings.max_warnings + ' لـ ' + esc(firstName);
}

function hasLink(text, whitelist) {
  if (!text) return false;
  if (!/https?:\/\/|t\.me\/|@[a-zA-Z]/i.test(text)) return false;
  if (whitelist && whitelist.length > 0) {
    const lower = text.toLowerCase();
    for (const allowed of whitelist) {
      if (lower.includes(allowed.toLowerCase())) return false;
    }
  }
  return true;
}

function esc(text) {
  return (text || '').replace(/&/g, 'and').replace(/</g, '(').replace(/>/g, ')');
}

async function banUserAndTrack(env, chatId, userId, firstName) {
  await banUser(env, chatId, userId);
  await addToBannedList(env, chatId, userId, firstName);
}

// ─── Telegram API Wrappers ────────────────────────────────────────────────────

async function sendMsg(env, chatId, text, keyboard) {
  const body = { chat_id: chatId, text: text };
  if (keyboard) body.reply_markup = keyboard;
  const r = await fetch(API(env) + '/sendMessage', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  return (await r.json()).result;
}

async function sendPhoto(env, chatId, fileId, caption, keyboard) {
  const body = { chat_id: chatId, photo: fileId, caption: caption || '' };
  if (keyboard) body.reply_markup = keyboard;
  const r = await fetch(API(env) + '/sendPhoto', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  return (await r.json()).result;
}

async function editMsg(env, chatId, msgId, text, keyboard) {
  const body = { chat_id: chatId, message_id: msgId, text: text };
  if (keyboard) body.reply_markup = keyboard;
  await fetch(API(env) + '/editMessageText', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
}

async function deleteMsg(env, chatId, msgId) {
  await fetch(API(env) + '/deleteMessage', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ chat_id: chatId, message_id: msgId }) });
}

async function pinMessage(env, chatId, msgId) {
  await fetch(API(env) + '/pinChatMessage', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ chat_id: chatId, message_id: msgId, disable_notification: false }) });
}

async function unpinMessage(env, chatId, msgId) {
  await fetch(API(env) + '/unpinChatMessage', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ chat_id: chatId, message_id: msgId }) });
}

async function answerCb(env, id, text, showAlert) {
  const body = { callback_query_id: id };
  if (text) { body.text = text; body.show_alert = !!showAlert; }
  await fetch(API(env) + '/answerCallbackQuery', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
}

async function setMuteData(env, chatId, userId, untilTimestamp) {
  try { await env.USER_DATA.put('mute_' + chatId + '_' + userId, untilTimestamp.toString(), { expirationTtl: TTL }); } catch {}
}

async function getMuteUntil(env, chatId, userId) {
  try { const d = await env.USER_DATA.get('mute_' + chatId + '_' + userId); return d ? parseInt(d) : null; } catch { return null; }
}

async function getChatMember(env, chatId, userId) {
  const r = await fetch(API(env) + '/getChatMember', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ chat_id: chatId, user_id: userId }) });
  const d = await r.json();
  return d.ok ? d.result : null;
}

async function banUser(env, chatId, userId) {
  await fetch(API(env) + '/banChatMember', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ chat_id: chatId, user_id: userId }) });
}

async function unbanUser(env, chatId, userId) {
  await fetch(API(env) + '/unbanChatMember', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ chat_id: chatId, user_id: userId }) });
}

async function kickUser(env, chatId, userId) {
  await banUser(env, chatId, userId);
  await unbanUser(env, chatId, userId);
}

async function restrictUser(env, chatId, userId, canSend, duration) {
  const until = duration ? Math.floor(Date.now() / 1000) + duration : 0;
  const perms = { can_send_messages: canSend, can_send_media_messages: canSend, can_send_other_messages: canSend, can_add_web_page_previews: canSend };
  await fetch(API(env) + '/restrictChatMember', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ chat_id: chatId, user_id: userId, permissions: perms, until_date: until }) });
  return until;
}

async function setChatPermissions(env, chatId, canSend) {
  const perms = { can_send_messages: canSend, can_send_media_messages: canSend, can_send_other_messages: canSend, can_add_web_page_previews: canSend, can_send_polls: canSend, can_invite_users: true, can_pin_messages: false, can_change_info: false };
  await fetch(API(env) + '/setChatPermissions', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ chat_id: chatId, permissions: perms }) });
}

async function setChatSlowModeDelay(env, chatId, delay) {
  await fetch(API(env) + '/setChatSlowModeDelay', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ chat_id: chatId, slow_mode_delay: delay }) });
}

async function applyMediaPermissions(env, chatId, settings) {
  const ml = settings.media_lock;
  const noOther = ml.sticker || ml.gif;
  const perms = {
    can_send_messages: true,
    can_send_photos: !ml.photo,
    can_send_videos: !ml.video,
    can_send_audios: !ml.audio,
    can_send_documents: !ml.document,
    can_send_voice_notes: !ml.mic,
    can_send_video_notes: !ml.video,
    can_send_other_messages: !noOther,
    can_add_web_page_previews: !settings.links_locked,
    can_send_polls: !ml.poll,
    can_invite_users: true,
    can_pin_messages: false,
    can_change_info: false,
  };
  await fetch(API(env) + '/setChatPermissions', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ chat_id: chatId, permissions: perms }) });
}

function formatMuteExpiry(durSecs) {
  const endDate = new Date(Date.now() + durSecs * 1000);
  const h = endDate.getUTCHours() + 3;
  const hh = ((h % 24) < 10 ? '0' : '') + (h % 24);
  const mm = (endDate.getUTCMinutes() < 10 ? '0' : '') + endDate.getUTCMinutes();
  let label = '';
  if (durSecs < 3600) label = Math.round(durSecs / 60) + ' دقيقة';
  else if (durSecs < 86400) label = Math.round(durSecs / 3600) + ' ساعة';
  else label = Math.round(durSecs / 86400) + ' يوم';
  return { label, time: hh + ':' + mm };
}
