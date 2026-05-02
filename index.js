// ═══════════════════════════════════════════════════════════════════════════
//  Telegram Group Management Bot — Cloudflare Worker
// ═══════════════════════════════════════════════════════════════════════════

export default {
  async fetch(request, env) {
    if (request.method !== 'POST') return new Response('OK', { status: 200 });
    try {
      const update = await request.json();
      await processUpdate(update, env);
    } catch (e) {
      console.error('processUpdate error:', e);
    }
    return new Response('OK', { status: 200 });
  },
};

// ─── Constants ───────────────────────────────────────────────────────────────

const API = (env) => `https://api.telegram.org/bot${env.BOT_TOKEN}`;
const TTL = 60 * 60 * 24 * 365;

// ─── Default Settings ────────────────────────────────────────────────────────

function defaultSettings() {
  return {
    group_locked:  false,
    links_locked:  false,
    anti_forward:  false,
    anti_channel:  false,

    media_lock: {
      photo: false, video: false, sticker: false,
      audio: false, mic:   false, gif:     false,
      document: false, poll: false,
    },

    warnings_enabled:      true,
    max_warnings:          3,
    warn_action:           'kick',

    banned_words: [],
    auto_replies: {},

    welcome_enabled: false,
    welcome_message: 'أهلاً {name} في {group} 🎉',
    captcha_enabled: false,

    night_mode: { enabled: false, start: 23, end: 6 },

    default_mute_duration: 3600,
    sub_admins: [],

    // ── جديد ──────────────────────────────────────────────────────────────
    anti_spam: {
      enabled:  false,
      max_msgs: 5,
      window:   10,
      action:   'mute',
    },
    anti_mention: {
      enabled:      false,
      max_mentions: 5,
    },
    log_channel: null,
    slow_mode:   0,
  };
}

// ─── KV Helpers ──────────────────────────────────────────────────────────────

async function getSettings(env, chatId) {
  try {
    const raw  = await env.GROUP_SETTINGS.get('s_' + chatId);
    if (!raw) return defaultSettings();
    const saved = JSON.parse(raw);
    const def   = defaultSettings();
    return {
      ...def,
      ...saved,
      media_lock:   { ...def.media_lock,   ...(saved.media_lock   || {}) },
      night_mode:   { ...def.night_mode,   ...(saved.night_mode   || {}) },
      anti_spam:    { ...def.anti_spam,    ...(saved.anti_spam    || {}) },
      anti_mention: { ...def.anti_mention, ...(saved.anti_mention || {}) },
      sub_admins:   saved.sub_admins || [],
    };
  } catch { return defaultSettings(); }
}

async function saveSettings(env, chatId, s) {
  try { await env.GROUP_SETTINGS.put('s_' + chatId, JSON.stringify(s), { expirationTtl: TTL }); } catch {}
}

async function getWarnings(env, chatId, userId) {
  try { const d = await env.USER_DATA.get('w_' + chatId + '_' + userId); return d ? parseInt(d) : 0; } catch { return 0; }
}
async function setWarnings(env, chatId, userId, count) {
  try { await env.USER_DATA.put('w_' + chatId + '_' + userId, count.toString(), { expirationTtl: TTL }); } catch {}
}

async function getUserGroups(env, userId) {
  try { const d = await env.USER_DATA.get('groups_' + userId); return d ? JSON.parse(d) : {}; } catch { return {}; }
}
async function addUserGroup(env, userId, chatId, chatTitle) {
  try {
    const groups = await getUserGroups(env, userId);
    groups[chatId.toString()] = chatTitle || chatId.toString();
    await env.USER_DATA.put('groups_' + userId, JSON.stringify(groups), { expirationTtl: TTL });
  } catch {}
}

async function getPending(env, userId) {
  try { const d = await env.USER_DATA.get('pend_' + userId); return d ? JSON.parse(d) : null; } catch { return null; }
}
async function setPending(env, userId, state) {
  try {
    if (state === null) await env.USER_DATA.delete('pend_' + userId);
    else await env.USER_DATA.put('pend_' + userId, JSON.stringify(state), { expirationTtl: 3600 });
  } catch {}
}

async function getAnnouncements(env, chatId) {
  try { const d = await env.USER_DATA.get('ann_' + chatId); return d ? JSON.parse(d) : []; } catch { return []; }
}
async function saveAnnouncements(env, chatId, list) {
  try { await env.USER_DATA.put('ann_' + chatId, JSON.stringify(list), { expirationTtl: TTL }); } catch {}
}

// ── Anti-spam ─────────────────────────────────────────────────────────────────
async function getSpamData(env, chatId, userId) {
  try { const d = await env.USER_DATA.get('spam_' + chatId + '_' + userId); return d ? JSON.parse(d) : []; } catch { return []; }
}
async function setSpamData(env, chatId, userId, timestamps) {
  try { await env.USER_DATA.put('spam_' + chatId + '_' + userId, JSON.stringify(timestamps), { expirationTtl: 120 }); } catch {}
}

// ── Ban list ──────────────────────────────────────────────────────────────────
async function getBanList(env, chatId) {
  try { const d = await env.GROUP_SETTINGS.get('banlist_' + chatId); return d ? JSON.parse(d) : {}; } catch { return {}; }
}
async function saveBanList(env, chatId, list) {
  try { await env.GROUP_SETTINGS.put('banlist_' + chatId, JSON.stringify(list), { expirationTtl: TTL }); } catch {}
}

// ─── Permissions Helpers ─────────────────────────────────────────────────────

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

async function registerAdmins(env, chatId, chatTitle) {
  try {
    const r = await fetch(API(env) + '/getChatAdministrators', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: chatId }),
    });
    const d = await r.json();
    if (d.ok) for (const admin of d.result)
      if (!admin.user.is_bot) await addUserGroup(env, admin.user.id, chatId, chatTitle);
  } catch {}
}

// ─── Main Processor ───────────────────────────────────────────────────────────

async function processUpdate(update, env) {
  if (update.message)            await handleMessage(update.message, env);
  else if (update.callback_query) await handleCallback(update.callback_query, env);
}

// ═══════════════════════════════════════════════════════════════════════════
//  handleMessage
// ═══════════════════════════════════════════════════════════════════════════

async function handleMessage(msg, env) {
  const chatId   = msg.chat.id;
  const userId   = msg.from ? msg.from.id : null;
  const text     = msg.text || '';
  const chatType = msg.chat.type;
  const msgId    = msg.message_id;

  if (!userId) return;

  // ── محادثة خاصة ───────────────────────────────────────────────────────────
  if (chatType === 'private') {
    const pending = await getPending(env, userId);

    if (pending) {

      // انتظار كلمة محظورة
      if (pending.step === 'await_banned_word') {
        const gChatId = pending.gChatId;
        if (!await isAuthorized(env, gChatId, userId)) { await setPending(env, userId, null); await sendMsg(env, chatId, '❌ انتهت صلاحيتك.'); return; }
        const word = text.trim();
        if (!word) { await sendMsg(env, chatId, 'أرسل الكلمة المراد حظرها:'); return; }
        const settings = await getSettings(env, gChatId);
        if (settings.banned_words.includes(word)) {
          await setPending(env, userId, null);
          await sendMsg(env, chatId, '⚠️ هذه الكلمة موجودة مسبقاً.', { inline_keyboard: [[{ text: '🔙 رجوع', callback_data: 'words_' + gChatId }]] });
          return;
        }
        settings.banned_words.push(word);
        await saveSettings(env, gChatId, settings);
        await setPending(env, userId, null);
        await sendMsg(env, chatId, '✅ تمت إضافة الكلمة المحظورة: "' + word + '"',
          { inline_keyboard: [[{ text: '🚫 إدارة الكلمات المحظورة', callback_data: 'words_' + gChatId }], [{ text: '🔙 رجوع للقائمة', callback_data: 'menu_' + gChatId }]] });
        return;
      }

      // انتظار رسالة الترحيب
      if (pending.step === 'await_welcome_msg') {
        const gChatId = pending.gChatId;
        if (!await isAuthorized(env, gChatId, userId)) { await setPending(env, userId, null); await sendMsg(env, chatId, '❌ انتهت صلاحيتك.'); return; }
        const newMsg = text.trim();
        if (!newMsg) { await sendMsg(env, chatId, 'أرسل نص رسالة الترحيب:\n\n💡 استخدم {name} لاسم العضو و {group} لاسم المجموعة'); return; }
        const settings = await getSettings(env, gChatId);
        settings.welcome_message = newMsg;
        await saveSettings(env, gChatId, settings);
        await setPending(env, userId, null);
        await sendMsg(env, chatId, '✅ تم تحديث رسالة الترحيب!\n\nالرسالة الجديدة:\n' + esc(newMsg),
          { inline_keyboard: [[{ text: '👋 إعدادات الترحيب', callback_data: 'welcome_' + gChatId }], [{ text: '🔙 رجوع للقائمة', callback_data: 'menu_' + gChatId }]] });
        return;
      }

      // انتظار ساعة بداية الوضع الليلي
      if (pending.step === 'await_night_start') {
        const gChatId = pending.gChatId;
        if (!await isAuthorized(env, gChatId, userId)) { await setPending(env, userId, null); await sendMsg(env, chatId, '❌ انتهت صلاحيتك.'); return; }
        const hour = parseInt(text.trim());
        if (isNaN(hour) || hour < 0 || hour > 23) { await sendMsg(env, chatId, '❌ أرسل رقم صحيح بين 0 و 23:'); return; }
        await setPending(env, userId, { step: 'await_night_end', gChatId, night_start: hour });
        await sendMsg(env, chatId, '✅ ساعة البداية: ' + hour + ':00\n\nالآن أرسل ساعة الانتهاء (0-23):');
        return;
      }

      // انتظار ساعة نهاية الوضع الليلي
      if (pending.step === 'await_night_end') {
        const gChatId = pending.gChatId;
        if (!await isAuthorized(env, gChatId, userId)) { await setPending(env, userId, null); await sendMsg(env, chatId, '❌ انتهت صلاحيتك.'); return; }
        const hour = parseInt(text.trim());
        if (isNaN(hour) || hour < 0 || hour > 23) { await sendMsg(env, chatId, '❌ أرسل رقم صحيح بين 0 و 23:'); return; }
        const settings = await getSettings(env, gChatId);
        settings.night_mode.start = pending.night_start;
        settings.night_mode.end   = hour;
        await saveSettings(env, gChatId, settings);
        await setPending(env, userId, null);
        await sendMsg(env, chatId, '✅ تم تحديث وقت الوضع الليلي!\nمن الساعة ' + pending.night_start + ':00 حتى ' + hour + ':00',
          { inline_keyboard: [[{ text: '🌙 إعدادات الوضع الليلي', callback_data: 'night_' + gChatId }], [{ text: '🔙 رجوع للقائمة', callback_data: 'menu_' + gChatId }]] });
        return;
      }

      // انتظار @username لإضافة مشرف فرعي
      if (pending.step === 'await_sub_admin_username') {
        const gChatId = pending.gChatId;
        if (!await isOwner(env, gChatId, userId)) { await setPending(env, userId, null); await sendMsg(env, chatId, '❌ هذه الخاصية للمالك فقط.'); return; }
        const input = text.trim().replace(/^@/, '');
        if (!input) { await sendMsg(env, chatId, '❌ أرسل اسم المستخدم بشكل صحيح مثال: @username'); return; }
        const userInfo = await getUserByUsername(env, input);
        if (!userInfo) {
          await sendMsg(env, chatId, '❌ لم يُعثر على المستخدم @' + input + '\n\nتأكد من صحة الـ username.',
            { inline_keyboard: [[{ text: '❌ إلغاء', callback_data: 'subadmins_' + gChatId }]] });
          return;
        }
        const memberInfo = await getChatMember(env, parseInt(gChatId), userInfo.id);
        if (!memberInfo || memberInfo.status === 'left' || memberInfo.status === 'kicked') {
          await sendMsg(env, chatId, '❌ المستخدم @' + (userInfo.username || input) + ' ليس عضواً في المجموعة.',
            { inline_keyboard: [[{ text: '❌ إلغاء', callback_data: 'subadmins_' + gChatId }]] });
          return;
        }
        const settings = await getSettings(env, gChatId);
        const subId = userInfo.id.toString();
        if (!settings.sub_admins.includes(subId)) {
          settings.sub_admins.push(subId);
          await saveSettings(env, gChatId, settings);
          await addUserGroup(env, userInfo.id, gChatId, pending.groupName || gChatId);
        }
        await setPending(env, userId, null);
        await sendMsg(env, chatId, '✅ تمت إضافة المشرف الفرعي!\n\nالاسم: ' + esc(userInfo.first_name) + '\nID: ' + subId,
          { inline_keyboard: [[{ text: '👮 إدارة المشرفين الفرعيين', callback_data: 'subadmins_' + gChatId }], [{ text: '🔙 رجوع للقائمة', callback_data: 'menu_' + gChatId }]] });
        return;
      }

      // انتظار محتوى الإعلان
      if (pending.step === 'await_ann_content') {
        const gChatId = pending.gChatId;
        if (!await isAuthorized(env, gChatId, userId)) { await setPending(env, userId, null); await sendMsg(env, chatId, '❌ أنت لست مشرفاً.'); return; }
        let photoFileId = null;
        let annText = msg.caption || msg.text || '';
        if (msg.photo && msg.photo.length > 0) photoFileId = msg.photo[msg.photo.length - 1].file_id;
        if (!annText && !photoFileId) { await sendMsg(env, chatId, '❌ الرجاء إرسال نص أو صورة.'); return; }
        await setPending(env, userId, { step: 'confirm_ann', gChatId, annText, photoFileId: photoFileId || null, orig_msg_id: pending.orig_msg_id, orig_chat_id: pending.orig_chat_id });
        const previewCaption = '📋 معاينة الإعلان:\n\n' + (annText || '(بدون نص)') + '\n\nهل تريد إرساله وتثبيته؟';
        const kb = { inline_keyboard: [[{ text: '📌 إرسال وتثبيت', callback_data: 'ann_confirm_' + gChatId }], [{ text: '❌ إلغاء', callback_data: 'ann_cancel_' + gChatId }]] };
        if (photoFileId) await sendPhoto(env, chatId, photoFileId, previewCaption, kb);
        else await sendMsg(env, chatId, previewCaption, kb);
        return;
      }

      // انتظار معرف قناة السجل
      if (pending.step === 'await_log_channel') {
        const gChatId = pending.gChatId;
        if (!await isAuthorized(env, gChatId, userId)) { await setPending(env, userId, null); await sendMsg(env, chatId, '❌ انتهت صلاحيتك.'); return; }
        const input = text.trim().replace(/^@/, '');
        if (!input) { await sendMsg(env, chatId, 'أرسل معرّف القناة (مثال: @mychannel):'); return; }
        const channelInfo = await getUserByUsername(env, input);
        if (!channelInfo || channelInfo.type !== 'channel') {
          await sendMsg(env, chatId, '❌ لم يتم العثور على القناة.\nتأكد من الـ username وأن البوت مشرف فيها.',
            { inline_keyboard: [[{ text: '❌ إلغاء', callback_data: 'log_' + gChatId }]] });
          return;
        }
        const settings = await getSettings(env, gChatId);
        settings.log_channel = channelInfo.id.toString();
        await saveSettings(env, gChatId, settings);
        await setPending(env, userId, null);
        await sendMsg(env, chatId, '✅ تم تعيين قناة السجل: ' + esc(channelInfo.title),
          { inline_keyboard: [[{ text: '🗒️ إعدادات قناة السجل', callback_data: 'log_' + gChatId }], [{ text: '🔙 رجوع للقائمة', callback_data: 'menu_' + gChatId }]] });
        return;
      }
    }

    // /start
    if (text === '/start') {
      await sendMsg(env, chatId, 'أهلاً بك في بوت إدارة المجموعات 🤖\n\nأضف البوت لمجموعتك كمشرف ثم اضغط زر مجموعاتي',
        { inline_keyboard: [[{ text: '👥 مجموعاتي', callback_data: 'my_groups' }]] });
    }
    return;
  }

  // ── مجموعات فقط ───────────────────────────────────────────────────────────
  if (chatType !== 'group' && chatType !== 'supergroup') return;

  const member   = await getChatMember(env, chatId, userId);
  const isAdmin  = member && (member.status === 'creator' || member.status === 'administrator');

  await registerAdmins(env, chatId, msg.chat.title || '');

  const settings = await getSettings(env, chatId);

  // ── أوامر قفل/فتح المجموعة ────────────────────────────────────────────────
  if (isAdmin && (text === 'قفل' || text === 'فتح')) {
    if (text === 'قفل') {
      settings.group_locked = true;
      await saveSettings(env, chatId, settings);
      await setChatPermissions(env, chatId, false);
      await sendMsg(env, chatId, '🔒 تم قفل المجموعة — لا يمكن لأي عضو الكتابة الآن.\nأرسل "فتح" لفتحها.');
    } else {
      settings.group_locked = false;
      await saveSettings(env, chatId, settings);
      await setChatPermissions(env, chatId, true);
      await sendMsg(env, chatId, '🔓 تم فتح المجموعة — يمكن للأعضاء الكتابة الآن.');
    }
    return;
  }

  // ── /report — لأي عضو (يرد على رسالة ويكتب /report) ─────────────────────
  if (text.startsWith('/report') && msg.reply_to_message) {
    await deleteMsg(env, chatId, msgId);
    const reported = msg.reply_to_message;
    const reportedName = reported.from ? esc(reported.from.first_name) : 'مجهول';
    const msgContent   = reported.text || reported.caption || '[وسائط]';
    await sendLog(env, settings, '📢 بلاغ جديد في ' + esc(msg.chat.title || '') +
      '\n\nالمُبلِّغ: ' + esc(msg.from.first_name) +
      '\nالمُبلَّغ عنه: ' + reportedName +
      '\nمحتوى الرسالة: ' + esc(msgContent.substring(0, 200)));
    await sendMsg(env, chatId, '📢 تم استقبال بلاغ ' + esc(msg.from.first_name) + '، شكراً لتعاونك.');
    return;
  }

  // ── استقبال أعضاء جدد (كابتشا رياضية) ───────────────────────────────────
  if (msg.new_chat_members) {
    for (const m of msg.new_chat_members) {
      if (m.is_bot) continue;
      if (settings.captcha_enabled) {
        await restrictUser(env, chatId, m.id, false);
        const math = genMath();
        const btns = math.opts.map(opt => ({
          text: String(opt),
          callback_data: (opt === math.answer ? 'capc_' : 'capw_') + m.id + '_' + chatId,
        }));
        await sendMsg(env, chatId,
          '👋 مرحباً ' + esc(m.first_name) + '\n\n🔢 حل المعادلة للتحقق من أنك لست روبوت:\n\n     ' + math.question + '\n\nلديك 5 دقائق للإجابة — اختر الرقم الصحيح:',
          { inline_keyboard: [btns] });
      } else if (settings.welcome_enabled) {
        const wmsg = settings.welcome_message
          .replace('{name}',  m.first_name)
          .replace('{group}', msg.chat.title || '');
        await sendMsg(env, chatId, wmsg);
      }
    }
    return;
  }

  // ── قفل المجموعة ─────────────────────────────────────────────────────────
  if (!isAdmin && settings.group_locked) { await deleteMsg(env, chatId, msgId); return; }

  // ── فحوصات رسائل الأعضاء العاديين ────────────────────────────────────────
  if (!isAdmin) {
    const name = esc(msg.from.first_name);

    // Anti-spam — منع تكرار الرسائل بسرعة
    if (settings.anti_spam.enabled) {
      const now        = Math.floor(Date.now() / 1000);
      const prev       = (await getSpamData(env, chatId, userId)).filter(t => now - t < settings.anti_spam.window);
      prev.push(now);
      await setSpamData(env, chatId, userId, prev);
      if (prev.length > settings.anti_spam.max_msgs) {
        await deleteMsg(env, chatId, msgId);
        const act = settings.anti_spam.action;
        if      (act === 'mute') await restrictUser(env, chatId, userId, false, 3600);
        else if (act === 'kick') await kickUser(env, chatId, userId);
        else if (act === 'ban')  await banUser(env, chatId, userId);
        await sendMsg(env, chatId, '🚫 ' + name + ' تجاوز حد الرسائل وتم ' + (act === 'mute' ? 'كتمه ساعة' : act === 'kick' ? 'طرده' : 'حظره') + ' تلقائياً');
        return;
      }
    }

    // Anti-mention — منع تاق عدد كبير من الأشخاص دفعة واحدة
    if (settings.anti_mention.enabled && msg.entities) {
      const mentionCount = msg.entities.filter(e => e.type === 'mention' || e.type === 'text_mention').length;
      if (mentionCount > settings.anti_mention.max_mentions) {
        await deleteMsg(env, chatId, msgId);
        await sendMsg(env, chatId, name + ' ❌ لا يمكن تاق أكثر من ' + settings.anti_mention.max_mentions + ' أشخاص دفعة واحدة');
        return;
      }
    }

    // روابط
    if (settings.links_locked && hasLink(text)) {
      await deleteMsg(env, chatId, msgId);
      const w = await applyWarning(env, chatId, userId, msg.from.first_name, settings);
      await sendMsg(env, chatId, name + ' ❌ الروابط ممنوعة!\n' + w);
      return;
    }

    // كلمات محظورة
    for (const bw of settings.banned_words) {
      if (text.toLowerCase().includes(bw.toLowerCase())) {
        await deleteMsg(env, chatId, msgId);
        const w = await applyWarning(env, chatId, userId, msg.from.first_name, settings);
        await sendMsg(env, chatId, name + ' ❌ رسالتك تحتوي على كلمة محظورة!\n' + w);
        return;
      }
    }

    // قفل الوسائط — حذف صامت (Telegram يمنع مسبقاً عبر setChatPermissions)
    const ml = settings.media_lock;
    if (ml.photo    && msg.photo)                     { await deleteMsg(env, chatId, msgId); return; }
    if (ml.video    && (msg.video || msg.video_note)) { await deleteMsg(env, chatId, msgId); return; }
    if (ml.sticker  && msg.sticker)                   { await deleteMsg(env, chatId, msgId); return; }
    if (ml.audio    && msg.audio)                     { await deleteMsg(env, chatId, msgId); return; }
    if (ml.mic      && msg.voice)                     { await deleteMsg(env, chatId, msgId); return; }
    if (ml.gif      && msg.animation)                 { await deleteMsg(env, chatId, msgId); return; }
    if (ml.document && msg.document)                  { await deleteMsg(env, chatId, msgId); return; }
    if (ml.poll     && msg.poll)                      { await deleteMsg(env, chatId, msgId); return; }

    // منع التوجيه
    if (settings.anti_forward && (msg.forward_from || msg.forward_from_chat)) {
      await deleteMsg(env, chatId, msgId);
      await sendMsg(env, chatId, name + ' ❌ التوجيه ممنوع');
      return;
    }

    // منع الكتابة باسم قناة
    if (settings.anti_channel && msg.sender_chat) { await deleteMsg(env, chatId, msgId); return; }

    // الوضع الليلي
    if (settings.night_mode.enabled) {
      const h  = (new Date().getUTCHours() + 3) % 24;
      const st = settings.night_mode.start;
      const en = settings.night_mode.end;
      const isNight = st > en ? (h >= st || h < en) : (h >= st && h < en);
      if (isNight) { await deleteMsg(env, chatId, msgId); await sendMsg(env, chatId, name + ' 🌙 المجموعة في الوضع الليلي'); return; }
    }
  }

  // ── الردود التلقائية ─────────────────────────────────────────────────────
  for (const [kw, reply] of Object.entries(settings.auto_replies)) {
    if (text.toLowerCase().includes(kw.toLowerCase())) { await sendMsg(env, chatId, reply); return; }
  }

  // ── أوامر المشرف ─────────────────────────────────────────────────────────
  if (isAdmin && text.startsWith('/')) {
    const parts = text.split(' ');
    const cmd   = parts[0].split('@')[0];
    const reply = msg.reply_to_message;

    if (cmd === '/ban' && reply) {
      const timeArg = parts[1];
      const secs    = timeArg ? parseTime(timeArg) : 0;
      await banUser(env, chatId, reply.from.id, secs);
      const bl = await getBanList(env, chatId);
      bl[reply.from.id.toString()] = { name: reply.from.first_name, date: new Date().toISOString() };
      await saveBanList(env, chatId, bl);
      const durText = secs ? ' لمدة ' + timeArg : ' بشكل دائم';
      await sendMsg(env, chatId, '🚫 تم حظر ' + esc(reply.from.first_name) + durText);
      await sendLog(env, settings, '🚫 حظر: ' + esc(reply.from.first_name) + ' بواسطة ' + esc(msg.from.first_name) + durText);

    } else if (cmd === '/unban' && reply) {
      await unbanUser(env, chatId, reply.from.id);
      const bl = await getBanList(env, chatId);
      delete bl[reply.from.id.toString()];
      await saveBanList(env, chatId, bl);
      await sendMsg(env, chatId, '✅ تم فك حظر ' + esc(reply.from.first_name));

    } else if (cmd === '/kick' && reply) {
      await kickUser(env, chatId, reply.from.id);
      await sendMsg(env, chatId, '👢 تم طرد ' + esc(reply.from.first_name));
      await sendLog(env, settings, '👢 طرد: ' + esc(reply.from.first_name) + ' بواسطة ' + esc(msg.from.first_name));

    } else if (cmd === '/mute' && reply) {
      const customMins = parseInt(parts[1]);
      const durSecs    = customMins ? customMins * 60 : (settings.default_mute_duration || 3600);
      const durMins    = Math.round(durSecs / 60);
      await restrictUser(env, chatId, reply.from.id, false, durSecs);
      await sendMsg(env, chatId, '🔇 تم كتم ' + esc(reply.from.first_name) + ' لمدة ' + durMins + ' دقيقة');
      await sendLog(env, settings, '🔇 كتم: ' + esc(reply.from.first_name) + ' لمدة ' + durMins + ' دقيقة بواسطة ' + esc(msg.from.first_name));

    } else if (cmd === '/unmute' && reply) {
      await restrictUser(env, chatId, reply.from.id, true);
      await sendMsg(env, chatId, '🔊 تم فك كتم ' + esc(reply.from.first_name));

    } else if (cmd === '/warn' && reply) {
      const result = await applyWarning(env, chatId, reply.from.id, reply.from.first_name, settings);
      await sendMsg(env, chatId, result);

    } else if (cmd === '/resetwarn' && reply) {
      await setWarnings(env, chatId, reply.from.id, 0);
      await sendMsg(env, chatId, '✅ تم مسح إنذارات ' + esc(reply.from.first_name));

    } else if (cmd === '/banlist') {
      const bl   = await getBanList(env, chatId);
      const keys = Object.keys(bl);
      if (keys.length === 0) { await sendMsg(env, chatId, '📋 لا يوجد أي محظور في هذه المجموعة'); }
      else {
        const list = keys.map((id, i) => (i + 1) + '. ' + esc(bl[id].name) + ' — ' + id).join('\n');
        await sendMsg(env, chatId, '🚫 قائمة المحظورين (' + keys.length + '):\n\n' + list);
      }

    } else if (cmd === '/addreply') {
      const kw = parts[1]; const rtext = parts.slice(2).join(' ');
      if (kw && rtext) { settings.auto_replies[kw] = rtext; await saveSettings(env, chatId, settings); await sendMsg(env, chatId, '✅ تم إضافة الرد التلقائي للكلمة: ' + kw); }

    } else if (cmd === '/removereply') {
      const kw = parts[1];
      if (kw) { delete settings.auto_replies[kw]; await saveSettings(env, chatId, settings); await sendMsg(env, chatId, '✅ تم حذف الرد التلقائي للكلمة: ' + kw); }
    }
  }
}

// ═══════════════════════════════════════════════════════════════════════════
//  handleCallback
// ═══════════════════════════════════════════════════════════════════════════

async function handleCallback(cb, env) {
  const data   = cb.data;
  const userId = cb.from.id;
  const chatId = cb.message.chat.id;
  const msgId  = cb.message.message_id;

  await answerCb(env, cb.id);

  // ── كابتشا رياضية — الإجابة الصحيحة ─────────────────────────────────────
  if (data.startsWith('capc_')) {
    const parts      = data.split('_');
    const targetId   = parts[1];
    const targetChat = parts[2];
    if (userId.toString() === targetId) {
      await restrictUser(env, parseInt(targetChat), parseInt(targetId), true);
      await deleteMsg(env, chatId, msgId);
      await sendMsg(env, parseInt(targetChat), '✅ ' + esc(cb.from.first_name) + ' أجاب بشكل صحيح وتم التحقق منه!');
    } else {
      await answerCb(env, cb.id, '❌ هذا السؤال ليس لك!');
    }
    return;
  }

  // ── كابتشا رياضية — الإجابة الخاطئة ─────────────────────────────────────
  if (data.startsWith('capw_')) {
    const parts    = data.split('_');
    const targetId = parts[1];
    if (userId.toString() === targetId) {
      await kickUser(env, parseInt(parts[2]), parseInt(targetId));
      await deleteMsg(env, chatId, msgId);
      await sendMsg(env, parseInt(parts[2]), '❌ ' + esc(cb.from.first_name) + ' أجاب بشكل خاطئ وتم طرده.');
    }
    return;
  }

  // ── كابتشا قديم (button) ─────────────────────────────────────────────────
  if (data.startsWith('cap_')) {
    const parts      = data.split('_');
    const targetId   = parts[1];
    const targetChat = parts[2];
    if (userId.toString() === targetId) {
      await restrictUser(env, parseInt(targetChat), parseInt(targetId), true);
      await deleteMsg(env, chatId, msgId);
      await sendMsg(env, parseInt(targetChat), 'مرحباً ' + esc(cb.from.first_name) + ' ✅ تم التحقق بنجاح!');
    }
    return;
  }

  // باقي الـ callbacks للمحادثة الخاصة فقط
  if (cb.message.chat.type !== 'private') return;

  // ── تأكيد الإعلان ────────────────────────────────────────────────────────
  if (data.startsWith('ann_confirm_')) {
    const gChatId = data.replace('ann_confirm_', '');
    const pending = await getPending(env, userId);
    if (!pending || pending.step !== 'confirm_ann') return;
    if (!await isAuthorized(env, gChatId, userId)) { await setPending(env, userId, null); await editMsg(env, chatId, msgId, '❌ لست مشرفاً.'); return; }
    let sentMsg = null;
    if (pending.photoFileId) sentMsg = await sendPhoto(env, parseInt(gChatId), pending.photoFileId, pending.annText || '');
    else sentMsg = await sendMsg(env, parseInt(gChatId), pending.annText);
    if (sentMsg && sentMsg.message_id) {
      await pinMessage(env, parseInt(gChatId), sentMsg.message_id);
      const anns = await getAnnouncements(env, gChatId);
      anns.push({ id: sentMsg.message_id, text: (pending.annText || '').substring(0, 80), hasPhoto: !!pending.photoFileId, date: new Date().toISOString() });
      await saveAnnouncements(env, gChatId, anns);
      await setPending(env, userId, null);
      await editMsg(env, chatId, msgId, '✅ تم إرسال الإعلان وتثبيته!',
        { inline_keyboard: [[{ text: '📌 إعلان جديد', callback_data: 'announce_' + gChatId }], [{ text: '🗑️ إدارة الإعلانات', callback_data: 'ann_list_' + gChatId }], [{ text: '🔙 رجوع', callback_data: 'menu_' + gChatId }]] });
    } else {
      await setPending(env, userId, null);
      await editMsg(env, chatId, msgId, '❌ حدث خطأ. تأكد أن البوت مشرف في المجموعة.',
        { inline_keyboard: [[{ text: '🔙 رجوع', callback_data: 'menu_' + gChatId }]] });
    }
    return;
  }

  if (data.startsWith('ann_cancel_')) {
    await setPending(env, userId, null);
    await editMsg(env, chatId, msgId, '❌ تم إلغاء الإعلان.',
      { inline_keyboard: [[{ text: '🔙 رجوع', callback_data: 'menu_' + data.replace('ann_cancel_', '') }]] });
    return;
  }

  if (data.startsWith('ann_list_')) {
    const gChatId = data.replace('ann_list_', '');
    if (!await isAuthorized(env, gChatId, userId)) return;
    await showAnnList(env, chatId, msgId, gChatId);
    return;
  }

  if (data.startsWith('ann_del_')) {
    const wp      = data.replace('ann_del_', '');
    const fU      = wp.indexOf('_');
    const idx     = parseInt(wp.substring(0, fU));
    const gChatId = wp.substring(fU + 1);
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
    await editMsg(env, chatId, msgId, '📌 إعلان جديد\n\nأرسل نص الإعلان أو صورة مع نص.',
      { inline_keyboard: [[{ text: '❌ إلغاء', callback_data: 'ann_cancel_' + gChatId }]] });
    return;
  }

  // ── مجموعاتي ─────────────────────────────────────────────────────────────
  if (data === 'my_groups') {
    const groups = await getUserGroups(env, userId);
    const keys   = Object.keys(groups);
    if (keys.length === 0) {
      await editMsg(env, chatId, msgId, 'ما عندك مجموعات مسجلة\nأضف البوت لمجموعتك كمشرف وأرسل أي رسالة فيها',
        { inline_keyboard: [[{ text: '🔄 تحديث', callback_data: 'my_groups' }]] });
      return;
    }
    const btns = keys.map(id => [{ text: groups[id], callback_data: 'grp_' + id }]);
    btns.push([{ text: '🔄 تحديث', callback_data: 'my_groups' }]);
    await editMsg(env, chatId, msgId, 'مجموعاتك:\nاختر مجموعة للإدارة:', { inline_keyboard: btns });
    return;
  }

  if (data.startsWith('grp_')) {
    const gId = data.replace('grp_', '');
    if (!await isAuthorized(env, gId, userId)) { await editMsg(env, chatId, msgId, '❌ أنت لست مشرفاً في هذه المجموعة'); return; }
    await showMainMenu(env, chatId, msgId, gId, userId);
    return;
  }

  // ── رفع الحظر من قائمة المحظورين ─────────────────────────────────────────
  if (data.startsWith('unban_')) {
    const parts    = data.replace('unban_', '').split('_');
    const targetId = parts[0];
    const gChatId  = parts.slice(1).join('_');
    if (!await isAuthorized(env, gChatId, userId)) return;
    await unbanUser(env, parseInt(gChatId), parseInt(targetId));
    const bl = await getBanList(env, parseInt(gChatId));
    delete bl[targetId];
    await saveBanList(env, parseInt(gChatId), bl);
    await showBanListMenu(env, chatId, msgId, gChatId);
    return;
  }

  // ── استخراج gChatId ──────────────────────────────────────────────────────
  const lastUnder = data.lastIndexOf('_');
  const gChatId   = lastUnder !== -1 ? data.substring(lastUnder + 1) : '';

  if (!await isAuthorized(env, gChatId, userId)) return;

  const settings = await getSettings(env, gChatId);

  // ── Routing ───────────────────────────────────────────────────────────────
  if (data === 'menu_'      + gChatId) { await setPending(env, userId, null); await showMainMenu(env, chatId, msgId, gChatId, userId);                return; }
  if (data === 'welcome_'   + gChatId) { await setPending(env, userId, null); await showWelcomeMenu(env, chatId, msgId, gChatId, settings);            return; }
  if (data === 'protect_'   + gChatId) { await setPending(env, userId, null); await showProtectMenu(env, chatId, msgId, gChatId, settings);            return; }
  if (data === 'media_'     + gChatId) { await setPending(env, userId, null); await showMediaMenu(env, chatId, msgId, gChatId, settings);              return; }
  if (data === 'warns_'     + gChatId) { await setPending(env, userId, null); await showWarnsMenu(env, chatId, msgId, gChatId, settings);              return; }
  if (data === 'night_'     + gChatId) { await setPending(env, userId, null); await showNightMenu(env, chatId, msgId, gChatId, settings);              return; }
  if (data === 'replies_'   + gChatId) { await setPending(env, userId, null); await showRepliesMenu(env, chatId, msgId, gChatId, settings);            return; }
  if (data === 'words_'     + gChatId) { await setPending(env, userId, null); await showWordsMenu(env, chatId, msgId, gChatId, settings);              return; }
  if (data === 'subadmins_' + gChatId) { await setPending(env, userId, null); await showSubAdminsMenu(env, chatId, msgId, gChatId, settings, userId);  return; }
  if (data === 'mute_menu_' + gChatId) { await setPending(env, userId, null); await showMuteMenu(env, chatId, msgId, gChatId, settings);               return; }
  if (data === 'spam_'      + gChatId) { await setPending(env, userId, null); await showAntiSpamMenu(env, chatId, msgId, gChatId, settings);           return; }
  if (data === 'slow_menu_' + gChatId) { await setPending(env, userId, null); await showSlowModeMenu(env, chatId, msgId, gChatId, settings);           return; }
  if (data === 'log_'       + gChatId) { await setPending(env, userId, null); await showLogChannelMenu(env, chatId, msgId, gChatId, settings);         return; }
  if (data === 'banlist_'   + gChatId) { await setPending(env, userId, null); await showBanListMenu(env, chatId, msgId, gChatId);                      return; }

  // ── إضافة كلمة محظورة ────────────────────────────────────────────────────
  if (data === 'add_word_' + gChatId) {
    await setPending(env, userId, { step: 'await_banned_word', gChatId });
    await editMsg(env, chatId, msgId, '🚫 إضافة كلمة محظورة\n\nأرسل الكلمة التي تريد حظرها:',
      { inline_keyboard: [[{ text: '❌ إلغاء', callback_data: 'words_' + gChatId }]] });
    return;
  }

  if (data.startsWith('del_word_')) {
    const wp      = data.replace('del_word_', '');
    const fU      = wp.indexOf('_');
    const wordIdx = parseInt(wp.substring(0, fU));
    const wGChatId = wp.substring(fU + 1);
    if (!await isAuthorized(env, wGChatId, userId)) return;
    const wSettings = await getSettings(env, wGChatId);
    if (!isNaN(wordIdx) && wordIdx >= 0 && wordIdx < wSettings.banned_words.length) {
      wSettings.banned_words.splice(wordIdx, 1);
      await saveSettings(env, wGChatId, wSettings);
    }
    await showWordsMenu(env, chatId, msgId, wGChatId, wSettings);
    return;
  }

  // ── تعديل رسالة الترحيب ───────────────────────────────────────────────────
  if (data === 'edit_welcome_' + gChatId) {
    await setPending(env, userId, { step: 'await_welcome_msg', gChatId });
    await editMsg(env, chatId, msgId, '✏️ تعديل رسالة الترحيب\n\nأرسل النص الجديد:\n\n💡 {name} = اسم العضو\n💡 {group} = اسم المجموعة',
      { inline_keyboard: [[{ text: '❌ إلغاء', callback_data: 'welcome_' + gChatId }]] });
    return;
  }

  // ── تعديل وقت الوضع الليلي ───────────────────────────────────────────────
  if (data === 'edit_night_time_' + gChatId) {
    await setPending(env, userId, { step: 'await_night_start', gChatId });
    await editMsg(env, chatId, msgId, '🌙 تعديل وقت الوضع الليلي\n\nأرسل ساعة البداية (0-23):\nمثال: 23 تعني 11 مساءً',
      { inline_keyboard: [[{ text: '❌ إلغاء', callback_data: 'night_' + gChatId }]] });
    return;
  }

  // ── إضافة مشرف فرعي ──────────────────────────────────────────────────────
  if (data === 'add_subadmin_' + gChatId) {
    if (!await isOwner(env, gChatId, userId)) {
      await editMsg(env, chatId, msgId, '❌ هذه الخاصية للمالك فقط.', { inline_keyboard: [[{ text: '🔙 رجوع', callback_data: 'subadmins_' + gChatId }]] });
      return;
    }
    const groups    = await getUserGroups(env, userId);
    const groupName = groups[gChatId.toString()] || gChatId;
    await setPending(env, userId, { step: 'await_sub_admin_username', gChatId, groupName });
    await editMsg(env, chatId, msgId, '👮 إضافة مشرف فرعي\n\nأرسل معرّف الشخص (مثال: @username)\n\n💡 يجب أن يكون عضواً في المجموعة',
      { inline_keyboard: [[{ text: '❌ إلغاء', callback_data: 'subadmins_' + gChatId }]] });
    return;
  }

  if (data.startsWith('del_subadmin_')) {
    const sp        = data.replace('del_subadmin_', '');
    const sFirstU   = sp.indexOf('_');
    const subIdx    = parseInt(sp.substring(0, sFirstU));
    const saGChatId = sp.substring(sFirstU + 1);
    if (!await isOwner(env, saGChatId, userId)) return;
    const saSettings = await getSettings(env, saGChatId);
    if (!isNaN(subIdx) && subIdx >= 0 && subIdx < saSettings.sub_admins.length) {
      saSettings.sub_admins.splice(subIdx, 1);
      await saveSettings(env, saGChatId, saSettings);
    }
    await showSubAdminsMenu(env, chatId, msgId, saGChatId, saSettings, userId);
    return;
  }

  // ── كتم بمدة محددة ───────────────────────────────────────────────────────
  if (data.startsWith('mute_dur_')) {
    const mp       = data.replace('mute_dur_', '').split('_');
    const durSecs  = parseInt(mp[0]);
    const targetId = parseInt(mp[1]);
    const mGChatId = mp.slice(2).join('_');
    if (!await isAuthorized(env, mGChatId, userId)) return;
    await restrictUser(env, parseInt(mGChatId), targetId, false, durSecs);
    const labels = { 600: '10 دقائق', 1800: '30 دقيقة', 3600: 'ساعة واحدة', 43200: '12 ساعة', 86400: '24 ساعة' };
    await editMsg(env, chatId, msgId, '🔇 تم كتم المستخدم لمدة ' + (labels[durSecs] || (durSecs / 60 + ' دقيقة')) + ' ✅',
      { inline_keyboard: [[{ text: '🔙 رجوع', callback_data: 'menu_' + mGChatId }]] });
    return;
  }

  if (data.startsWith('mute_def_')) {
    const mdp      = data.replace('mute_def_', '');
    const mdFirstU = mdp.indexOf('_');
    const durSecs  = parseInt(mdp.substring(0, mdFirstU));
    const mGChatId = mdp.substring(mdFirstU + 1);
    if (!await isAuthorized(env, mGChatId, userId)) return;
    const mSettings = await getSettings(env, mGChatId);
    mSettings.default_mute_duration = durSecs;
    await saveSettings(env, mGChatId, mSettings);
    await showMuteMenu(env, chatId, msgId, mGChatId, mSettings);
    return;
  }

  // ── إعداد قناة السجل ─────────────────────────────────────────────────────
  if (data === 'set_log_' + gChatId) {
    await setPending(env, userId, { step: 'await_log_channel', gChatId });
    await editMsg(env, chatId, msgId, '🗒️ تعيين قناة السجل\n\nأرسل معرّف القناة:\nمثال: @mychannel\n\n💡 تأكد من إضافة البوت كمشرف في القناة أولاً',
      { inline_keyboard: [[{ text: '❌ إلغاء', callback_data: 'log_' + gChatId }]] });
    return;
  }

  if (data === 'del_log_' + gChatId) {
    settings.log_channel = null;
    await saveSettings(env, gChatId, settings);
    await showLogChannelMenu(env, chatId, msgId, gChatId, settings);
    return;
  }

  // ── Slow Mode ─────────────────────────────────────────────────────────────
  if (data.startsWith('slow_') && data.endsWith('_' + gChatId)) {
    const secs = parseInt(data.replace('slow_', '').replace('_' + gChatId, ''));
    if (!isNaN(secs)) {
      settings.slow_mode = secs;
      await saveSettings(env, gChatId, settings);
      await setChatSlowMode(env, parseInt(gChatId), secs);
      await showSlowModeMenu(env, chatId, msgId, gChatId, settings);
    }
    return;
  }

  // ── عدد الإنذارات وإجراؤها ───────────────────────────────────────────────
  if (data.startsWith('wmax_')) { const num = parseInt(data.split('_')[1]); if (!isNaN(num)) { settings.max_warnings = num; await saveSettings(env, gChatId, settings); await showWarnsMenu(env, chatId, msgId, gChatId, settings); } return; }
  if (data.startsWith('wact_')) { const action = data.split('_')[1]; if (['kick','ban','mute'].includes(action)) { settings.warn_action = action; await saveSettings(env, gChatId, settings); await showWarnsMenu(env, chatId, msgId, gChatId, settings); } return; }

  // ── Anti-spam settings ────────────────────────────────────────────────────
  if (data.startsWith('smax_') && data.endsWith('_' + gChatId)) {
    const n = parseInt(data.replace('smax_', '').replace('_' + gChatId, ''));
    if (!isNaN(n)) { settings.anti_spam.max_msgs = n; await saveSettings(env, gChatId, settings); await showAntiSpamMenu(env, chatId, msgId, gChatId, settings); }
    return;
  }
  if (data.startsWith('swin_') && data.endsWith('_' + gChatId)) {
    const n = parseInt(data.replace('swin_', '').replace('_' + gChatId, ''));
    if (!isNaN(n)) { settings.anti_spam.window = n; await saveSettings(env, gChatId, settings); await showAntiSpamMenu(env, chatId, msgId, gChatId, settings); }
    return;
  }
  if (data.startsWith('sact_') && data.endsWith('_' + gChatId)) {
    const action = data.replace('sact_', '').replace('_' + gChatId, '');
    if (['mute','kick','ban'].includes(action)) { settings.anti_spam.action = action; await saveSettings(env, gChatId, settings); await showAntiSpamMenu(env, chatId, msgId, gChatId, settings); }
    return;
  }
  if (data.startsWith('mmax_') && data.endsWith('_' + gChatId)) {
    const n = parseInt(data.replace('mmax_', '').replace('_' + gChatId, ''));
    if (!isNaN(n)) { settings.anti_mention.max_mentions = n; await saveSettings(env, gChatId, settings); await showAntiSpamMenu(env, chatId, msgId, gChatId, settings); }
    return;
  }

  // ── Toggles ───────────────────────────────────────────────────────────────
  const toggles = {
    ['twelcome_'   + gChatId]: () => { settings.welcome_enabled        = !settings.welcome_enabled;      return () => showWelcomeMenu(env, chatId, msgId, gChatId, settings); },
    ['tcaptcha_'   + gChatId]: () => { settings.captcha_enabled        = !settings.captcha_enabled;      return () => showWelcomeMenu(env, chatId, msgId, gChatId, settings); },
    ['tlinks_'     + gChatId]: () => { settings.links_locked           = !settings.links_locked;         return () => showProtectMenu(env, chatId, msgId, gChatId, settings); },
    ['tforward_'   + gChatId]: () => { settings.anti_forward           = !settings.anti_forward;         return () => showProtectMenu(env, chatId, msgId, gChatId, settings); },
    ['tchannel_'   + gChatId]: () => { settings.anti_channel           = !settings.anti_channel;         return () => showProtectMenu(env, chatId, msgId, gChatId, settings); },
    ['twarns_'     + gChatId]: () => { settings.warnings_enabled       = !settings.warnings_enabled;     return () => showWarnsMenu(env, chatId, msgId, gChatId, settings); },
    ['tnight_'     + gChatId]: () => { settings.night_mode.enabled     = !settings.night_mode.enabled;   return () => showNightMenu(env, chatId, msgId, gChatId, settings); },
    ['tspam_'      + gChatId]: () => { settings.anti_spam.enabled      = !settings.anti_spam.enabled;    return () => showAntiSpamMenu(env, chatId, msgId, gChatId, settings); },
    ['tmention_'   + gChatId]: () => { settings.anti_mention.enabled   = !settings.anti_mention.enabled; return () => showAntiSpamMenu(env, chatId, msgId, gChatId, settings); },
    // الوسائط — تُطبَّق على صلاحيات المجموعة فوراً
    ['tphoto_'   + gChatId]: () => { settings.media_lock.photo    = !settings.media_lock.photo;    return async () => { await applyMediaPermissions(env, parseInt(gChatId), settings); await showMediaMenu(env, chatId, msgId, gChatId, settings); }; },
    ['tvideo_'   + gChatId]: () => { settings.media_lock.video    = !settings.media_lock.video;    return async () => { await applyMediaPermissions(env, parseInt(gChatId), settings); await showMediaMenu(env, chatId, msgId, gChatId, settings); }; },
    ['tsticker_' + gChatId]: () => { settings.media_lock.sticker  = !settings.media_lock.sticker;  return async () => { await applyMediaPermissions(env, parseInt(gChatId), settings); await showMediaMenu(env, chatId, msgId, gChatId, settings); }; },
    ['taudio_'   + gChatId]: () => { settings.media_lock.audio    = !settings.media_lock.audio;    return async () => { await applyMediaPermissions(env, parseInt(gChatId), settings); await showMediaMenu(env, chatId, msgId, gChatId, settings); }; },
    ['tmic_'     + gChatId]: () => { settings.media_lock.mic      = !settings.media_lock.mic;      return async () => { await applyMediaPermissions(env, parseInt(gChatId), settings); await showMediaMenu(env, chatId, msgId, gChatId, settings); }; },
    ['tgif_'     + gChatId]: () => { settings.media_lock.gif      = !settings.media_lock.gif;      return async () => { await applyMediaPermissions(env, parseInt(gChatId), settings); await showMediaMenu(env, chatId, msgId, gChatId, settings); }; },
    ['tdoc_'     + gChatId]: () => { settings.media_lock.document = !settings.media_lock.document; return async () => { await applyMediaPermissions(env, parseInt(gChatId), settings); await showMediaMenu(env, chatId, msgId, gChatId, settings); }; },
    ['tpoll_'    + gChatId]: () => { settings.media_lock.poll     = !settings.media_lock.poll;     return async () => { await applyMediaPermissions(env, parseInt(gChatId), settings); await showMediaMenu(env, chatId, msgId, gChatId, settings); }; },
  };

  if (toggles[data]) {
    const refresh = toggles[data]();
    await saveSettings(env, gChatId, settings);
    await refresh();
  }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Menu Renderers
// ═══════════════════════════════════════════════════════════════════════════

function on(v) { return v ? '✅' : '❌'; }

async function showMainMenu(env, chatId, msgId, gChatId, userId) {
  const groups    = await getUserGroups(env, userId);
  const groupName = groups[gChatId.toString()] || gChatId;
  await editMsg(env, chatId, msgId,
    '⚙️ إدارة: ' + esc(groupName) + '\n\nاختر القسم الذي تريد إدارته:',
    { inline_keyboard: [
      [{ text: '👋 الترحيب والكابتشا',     callback_data: 'welcome_'   + gChatId }],
      [{ text: '🤖 الردود التلقائية',      callback_data: 'replies_'   + gChatId }],
      [{ text: '🛡️ الحماية',              callback_data: 'protect_'   + gChatId }],
      [{ text: '🎬 قفل الوسائط',           callback_data: 'media_'     + gChatId }],
      [{ text: '📵 مكافحة السبام',         callback_data: 'spam_'      + gChatId }],
      [{ text: '⚠️ الإنذارات والعقوبات',  callback_data: 'warns_'     + gChatId }],
      [{ text: '🌙 الوضع الليلي',          callback_data: 'night_'     + gChatId }],
      [{ text: '🚫 الكلمات المحظورة',      callback_data: 'words_'     + gChatId }],
      [{ text: '🔇 إعدادات الكتم',         callback_data: 'mute_menu_' + gChatId }],
      [{ text: '🐢 Slow Mode',             callback_data: 'slow_menu_' + gChatId }],
      [{ text: '🗒️ قناة السجل',           callback_data: 'log_'       + gChatId }],
      [{ text: '🚫 قائمة المحظورين',       callback_data: 'banlist_'   + gChatId }],
      [{ text: '👮 المشرفون الفرعيون',     callback_data: 'subadmins_' + gChatId }],
      [{ text: '📌 تثبيت إعلان',           callback_data: 'announce_'  + gChatId }],
      [{ text: '🔙 رجوع لمجموعاتي',        callback_data: 'my_groups'             }],
    ]}
  );
}

async function showWelcomeMenu(env, chatId, msgId, gChatId, s) {
  await editMsg(env, chatId, msgId,
    '👋 الترحيب والكابتشا\n\n' +
    'الترحيب: ' + on(s.welcome_enabled) + ' — رسالة ترحيب لكل عضو جديد\n' +
    'الكابتشا الرياضية: ' + on(s.captcha_enabled) + ' — يُقيَّد العضو الجديد ويُطلب منه حل معادلة رياضية للتحقق، وإلا يُطرد\n\n' +
    'رسالة الترحيب الحالية:\n' + esc(s.welcome_message),
    { inline_keyboard: [
      [
        { text: on(s.welcome_enabled) + ' الترحيب',          callback_data: 'twelcome_' + gChatId },
        { text: on(s.captcha_enabled) + ' الكابتشا الرياضية', callback_data: 'tcaptcha_' + gChatId },
      ],
      [{ text: '✏️ تعديل رسالة الترحيب', callback_data: 'edit_welcome_' + gChatId }],
      [{ text: '🔙 رجوع',                callback_data: 'menu_'         + gChatId }],
    ]}
  );
}

async function showProtectMenu(env, chatId, msgId, gChatId, s) {
  await editMsg(env, chatId, msgId,
    '🛡️ الحماية\n\n' +
    '🔗 قفل الروابط — حذف أي رسالة تحتوي على رابط أو @ مع إنذار للمرسل\n' +
    '↩️ منع التوجيه — حذف الرسائل المحوَّلة من مجموعات/قنوات أخرى\n' +
    '📢 منع القنوات — منع الأشخاص من الكتابة باسم قناة داخل المجموعة',
    { inline_keyboard: [
      [{ text: on(s.links_locked) + ' 🔗 قفل الروابط',  callback_data: 'tlinks_'   + gChatId }],
      [{ text: on(s.anti_forward) + ' ↩️ منع التوجيه',  callback_data: 'tforward_' + gChatId }],
      [{ text: on(s.anti_channel) + ' 📢 منع القنوات',  callback_data: 'tchannel_' + gChatId }],
      [{ text: '🔙 رجوع', callback_data: 'menu_' + gChatId }],
    ]}
  );
}

async function showMediaMenu(env, chatId, msgId, gChatId, s) {
  const ml = s.media_lock;
  await editMsg(env, chatId, msgId,
    '🎬 قفل الوسائط\n\nعند التفعيل يُمنع إرسال النوع مباشرة من Telegram — يظهر للعضو "لا يُسمح بإرسال الوسائط"',
    { inline_keyboard: [
      [{ text: on(ml.photo)    + ' صور 📷',    callback_data: 'tphoto_'   + gChatId }, { text: on(ml.video)    + ' فيديو 🎥',    callback_data: 'tvideo_'   + gChatId }],
      [{ text: on(ml.sticker)  + ' ملصقات 🗿', callback_data: 'tsticker_' + gChatId }, { text: on(ml.audio)    + ' صوت 🎵',      callback_data: 'taudio_'   + gChatId }],
      [{ text: on(ml.mic)      + ' مايك 🎙',   callback_data: 'tmic_'     + gChatId }, { text: on(ml.gif)      + ' GIF 🎞',      callback_data: 'tgif_'     + gChatId }],
      [{ text: on(ml.document) + ' ملفات 📁',  callback_data: 'tdoc_'     + gChatId }, { text: on(ml.poll)     + ' استطلاع 📊', callback_data: 'tpoll_'    + gChatId }],
      [{ text: '🔙 رجوع', callback_data: 'menu_' + gChatId }],
    ]}
  );
}

async function showAntiSpamMenu(env, chatId, msgId, gChatId, s) {
  const as   = s.anti_spam;
  const am   = s.anti_mention;
  const acts = { mute: 'كتم 🔇', kick: 'طرد 👢', ban: 'حظر 🚫' };
  const sel  = (cur, v, label) => cur === v ? '◀ ' + label + ' ▶' : label;
  await editMsg(env, chatId, msgId,
    '📵 مكافحة السبام\n\n' +
    '🔁 السبام التلقائي: ' + on(as.enabled) + '\n' +
    '   ▸ الحد: ' + as.max_msgs + ' رسائل في ' + as.window + ' ثواني\n' +
    '   ▸ العقوبة عند التجاوز: ' + acts[as.action] + '\n\n' +
    '🏷️ منع التاقات الجماعية: ' + on(am.enabled) + '\n' +
    '   ▸ الحد: أكثر من ' + am.max_mentions + ' تاقات في رسالة واحدة',
    { inline_keyboard: [
      [
        { text: on(as.enabled) + ' السبام التلقائي', callback_data: 'tspam_'    + gChatId },
        { text: on(am.enabled) + ' التاقات',         callback_data: 'tmention_' + gChatId },
      ],
      [
        { text: sel(as.max_msgs, 3,  '3 رسائل'),  callback_data: 'smax_3_'  + gChatId },
        { text: sel(as.max_msgs, 5,  '5 رسائل'),  callback_data: 'smax_5_'  + gChatId },
        { text: sel(as.max_msgs, 10, '10 رسائل'), callback_data: 'smax_10_' + gChatId },
      ],
      [
        { text: sel(as.window, 5,  '5 ثواني'),  callback_data: 'swin_5_'  + gChatId },
        { text: sel(as.window, 10, '10 ثواني'), callback_data: 'swin_10_' + gChatId },
        { text: sel(as.window, 30, '30 ثانية'), callback_data: 'swin_30_' + gChatId },
      ],
      [
        { text: sel(as.action, 'mute', 'كتم 🔇'),  callback_data: 'sact_mute_' + gChatId },
        { text: sel(as.action, 'kick', 'طرد 👢'),  callback_data: 'sact_kick_' + gChatId },
        { text: sel(as.action, 'ban',  'حظر 🚫'),  callback_data: 'sact_ban_'  + gChatId },
      ],
      [
        { text: sel(am.max_mentions, 3,  '3 تاقات'),  callback_data: 'mmax_3_'  + gChatId },
        { text: sel(am.max_mentions, 5,  '5 تاقات'),  callback_data: 'mmax_5_'  + gChatId },
        { text: sel(am.max_mentions, 10, '10 تاقات'), callback_data: 'mmax_10_' + gChatId },
      ],
      [{ text: '🔙 رجوع', callback_data: 'menu_' + gChatId }],
    ]}
  );
}

async function showSlowModeMenu(env, chatId, msgId, gChatId, s) {
  const cur    = s.slow_mode || 0;
  const labels = { 0: 'إيقاف ❌', 10: '10 ثواني', 30: '30 ثانية', 60: 'دقيقة', 300: '5 دقائق', 600: '10 دقائق', 900: '15 دقيقة' };
  const sel    = v => cur === v ? '◀ ' + labels[v] + ' ▶' : labels[v];
  await editMsg(env, chatId, msgId,
    '🐢 Slow Mode — الوضع البطيء\n\n' +
    'الحالة: ' + (cur === 0 ? '❌ مُعطّل' : '✅ ' + labels[cur] + ' بين كل رسالتين') + '\n\n' +
    'يمنع الأعضاء من الإرسال بسرعة كبيرة عن طريق تحديد فترة انتظار بين كل رسالتين.',
    { inline_keyboard: [
      [{ text: sel(0),   callback_data: 'slow_0_'   + gChatId }, { text: sel(10),  callback_data: 'slow_10_'  + gChatId }, { text: sel(30),  callback_data: 'slow_30_'  + gChatId }],
      [{ text: sel(60),  callback_data: 'slow_60_'  + gChatId }, { text: sel(300), callback_data: 'slow_300_' + gChatId }],
      [{ text: sel(600), callback_data: 'slow_600_' + gChatId }, { text: sel(900), callback_data: 'slow_900_' + gChatId }],
      [{ text: '🔙 رجوع', callback_data: 'menu_' + gChatId }],
    ]}
  );
}

async function showLogChannelMenu(env, chatId, msgId, gChatId, s) {
  const status = s.log_channel ? '✅ مفعّل\nالقناة: ' + s.log_channel : '❌ غير مفعّل';
  const btns   = [
    [{ text: s.log_channel ? '✏️ تغيير القناة' : '➕ تعيين قناة السجل', callback_data: 'set_log_' + gChatId }],
    ...(s.log_channel ? [[{ text: '🗑️ إلغاء قناة السجل', callback_data: 'del_log_' + gChatId }]] : []),
    [{ text: '🔙 رجوع', callback_data: 'menu_' + gChatId }],
  ];
  await editMsg(env, chatId, msgId,
    '🗒️ قناة السجل\n\n' + status + '\n\n' +
    'ترسل كل الأحداث (حظر، طرد، كتم، بلاغات) تلقائياً إلى القناة المحددة.\n\n' +
    '💡 أضف البوت كمشرف في القناة قبل تعيينها.',
    { inline_keyboard: btns }
  );
}

async function showBanListMenu(env, chatId, msgId, gChatId) {
  const bl      = await getBanList(env, parseInt(gChatId));
  const entries = Object.entries(bl);
  let body      = '🚫 قائمة المحظورين\n\n';
  const btns    = [];
  if (entries.length === 0) {
    body += 'لا يوجد أي محظور حالياً.';
  } else {
    entries.forEach(([uid, info], i) => {
      const date = info.date ? new Date(info.date).toLocaleDateString('ar-SA') : '—';
      body += (i + 1) + '. ' + esc(info.name) + '\n   ' + date + '\n';
      btns.push([{ text: '🔓 رفع حظر ' + esc(info.name), callback_data: 'unban_' + uid + '_' + gChatId }]);
    });
  }
  btns.push([{ text: '🔙 رجوع للقائمة', callback_data: 'menu_' + gChatId }]);
  await editMsg(env, chatId, msgId, body, { inline_keyboard: btns });
}

async function showWarnsMenu(env, chatId, msgId, gChatId, s) {
  const acts = { kick: 'طرد 👢', ban: 'حظر 🚫', mute: 'كتم 🔇' };
  await editMsg(env, chatId, msgId,
    '⚠️ الإنذارات والعقوبات\n\n' +
    'الحالة: ' + on(s.warnings_enabled) + '\n' +
    'الحد الأقصى: ' + s.max_warnings + ' إنذارات — بعدها تُطبَّق العقوبة تلقائياً\n' +
    'العقوبة: ' + acts[s.warn_action],
    { inline_keyboard: [
      [{ text: on(s.warnings_enabled) + ' تفعيل الإنذارات', callback_data: 'twarns_' + gChatId }],
      [
        { text: s.max_warnings === 2  ? '◀ 2 ▶'  : '2',  callback_data: 'wmax_2_'  + gChatId },
        { text: s.max_warnings === 3  ? '◀ 3 ▶'  : '3',  callback_data: 'wmax_3_'  + gChatId },
        { text: s.max_warnings === 5  ? '◀ 5 ▶'  : '5',  callback_data: 'wmax_5_'  + gChatId },
        { text: s.max_warnings === 10 ? '◀ 10 ▶' : '10', callback_data: 'wmax_10_' + gChatId },
      ],
      [
        { text: s.warn_action === 'kick' ? '◀ طرد ▶'  : 'طرد',  callback_data: 'wact_kick_' + gChatId },
        { text: s.warn_action === 'ban'  ? '◀ حظر ▶'  : 'حظر',  callback_data: 'wact_ban_'  + gChatId },
        { text: s.warn_action === 'mute' ? '◀ كتم ▶'  : 'كتم',  callback_data: 'wact_mute_' + gChatId },
      ],
      [{ text: '🔙 رجوع', callback_data: 'menu_' + gChatId }],
    ]}
  );
}

async function showNightMenu(env, chatId, msgId, gChatId, s) {
  await editMsg(env, chatId, msgId,
    '🌙 الوضع الليلي\n\n' +
    'الحالة: ' + on(s.night_mode.enabled) + '\n' +
    'من الساعة ' + s.night_mode.start + ':00 حتى ' + s.night_mode.end + ':00\n\n' +
    'خلال هذا الوقت تُحذف رسائل الأعضاء تلقائياً.',
    { inline_keyboard: [
      [{ text: on(s.night_mode.enabled) + ' تفعيل الوضع الليلي', callback_data: 'tnight_'         + gChatId }],
      [{ text: '⏰ تعديل الوقت',                                  callback_data: 'edit_night_time_' + gChatId }],
      [{ text: '🔙 رجوع',                                         callback_data: 'menu_'            + gChatId }],
    ]}
  );
}

async function showRepliesMenu(env, chatId, msgId, gChatId, s) {
  const list = Object.keys(s.auto_replies).length > 0
    ? Object.entries(s.auto_replies).map(([k, v]) => '• ' + k + ' ← ' + v).join('\n')
    : 'لا توجد ردود تلقائية';
  await editMsg(env, chatId, msgId,
    '🤖 الردود التلقائية\n\nعند كتابة الكلمة في المجموعة يرد البوت تلقائياً بالنص المحدد.\n\n' +
    list + '\n\n' +
    '▸ لإضافة رد أرسل في المجموعة: /addreply كلمة الرد\n' +
    '▸ لحذف رد: /removereply كلمة',
    { inline_keyboard: [[{ text: '🔙 رجوع', callback_data: 'menu_' + gChatId }]] }
  );
}

async function showWordsMenu(env, chatId, msgId, gChatId, s) {
  const wordsList = s.banned_words.length > 0
    ? s.banned_words.map((w, i) => (i + 1) + '. ' + w).join('\n')
    : 'لا توجد كلمات محظورة';
  const btns = [];
  s.banned_words.forEach((word, idx) => btns.push([{ text: '🗑️ حذف: ' + word, callback_data: 'del_word_' + idx + '_' + gChatId }]));
  btns.push([{ text: '➕ إضافة كلمة محظورة', callback_data: 'add_word_' + gChatId }]);
  btns.push([{ text: '🔙 رجوع',             callback_data: 'menu_'     + gChatId }]);
  await editMsg(env, chatId, msgId, '🚫 الكلمات المحظورة\n\nأي رسالة تحتوي على هذه الكلمات تُحذف وتُسجَّل كإنذار.\n\n' + wordsList, { inline_keyboard: btns });
}

async function showMuteMenu(env, chatId, msgId, gChatId, s) {
  const cur    = s.default_mute_duration || 3600;
  const labels = { 600: '10 دقائق', 1800: '30 دقيقة', 3600: 'ساعة', 43200: '12 ساعة', 86400: '24 ساعة' };
  const sel    = v => cur === v ? '◀ ' + labels[v] + ' ▶' : labels[v];
  await editMsg(env, chatId, msgId,
    '🔇 إعدادات الكتم\n\n' +
    'المدة الافتراضية: ' + (labels[cur] || (cur / 60 + ' دقيقة')) + '\n\n' +
    '▸ عند كتابة /mute بدون تحديد وقت تُستخدم هذه المدة\n' +
    '▸ لتحديد وقت مخصص: /mute 45 (بالدقائق)',
    { inline_keyboard: [
      [{ text: sel(600),   callback_data: 'mute_def_600_'   + gChatId }, { text: sel(1800),  callback_data: 'mute_def_1800_'  + gChatId }],
      [{ text: sel(3600),  callback_data: 'mute_def_3600_'  + gChatId }, { text: sel(43200), callback_data: 'mute_def_43200_' + gChatId }],
      [{ text: sel(86400), callback_data: 'mute_def_86400_' + gChatId }],
      [{ text: '🔙 رجوع', callback_data: 'menu_' + gChatId }],
    ]}
  );
}

async function showSubAdminsMenu(env, chatId, msgId, gChatId, s, userId) {
  const ownerCheck = await isOwner(env, gChatId, userId);
  const btns       = [];
  let bodyText     = '👮 المشرفون الفرعيون\n\n' +
    'المشرف الفرعي يمكنه إدارة إعدادات البوت (لا يملك صلاحيات Telegram الحقيقية).\n\n';
  if (s.sub_admins.length === 0) {
    bodyText += 'لا يوجد مشرفون فرعيون حالياً.';
  } else {
    s.sub_admins.forEach((id, idx) => {
      bodyText += (idx + 1) + '. ID: ' + id + '\n';
      if (ownerCheck) btns.push([{ text: '🗑️ حذف المشرف ' + (idx + 1), callback_data: 'del_subadmin_' + idx + '_' + gChatId }]);
    });
  }
  if (ownerCheck) btns.push([{ text: '➕ إضافة مشرف فرعي', callback_data: 'add_subadmin_' + gChatId }]);
  else bodyText += '\n⚠️ إضافة وحذف المشرفين الفرعيين للمالك فقط';
  btns.push([{ text: '🔙 رجوع', callback_data: 'menu_' + gChatId }]);
  await editMsg(env, chatId, msgId, bodyText, { inline_keyboard: btns });
}

async function showAnnList(env, chatId, msgId, gChatId, notice) {
  const anns   = await getAnnouncements(env, gChatId);
  let bodyText = (notice ? notice + '\n\n' : '') + '📌 الإعلانات المثبتة\n\n';
  const btns   = [];
  if (anns.length === 0) { bodyText += 'لا توجد إعلانات مسجلة.'; }
  else anns.forEach((a, i) => {
    const icon    = a.hasPhoto ? '🖼️' : '📝';
    const preview = a.text ? a.text.substring(0, 40) + (a.text.length > 40 ? '...' : '') : '(بدون نص)';
    bodyText += (i + 1) + '. ' + icon + ' ' + preview + '\n';
    btns.push([{ text: '🗑️ حذف الإعلان ' + (i + 1), callback_data: 'ann_del_' + i + '_' + gChatId }]);
  });
  btns.push([{ text: '📌 إضافة إعلان جديد', callback_data: 'announce_' + gChatId }]);
  btns.push([{ text: '🔙 رجوع للقائمة',     callback_data: 'menu_'     + gChatId }]);
  await editMsg(env, chatId, msgId, bodyText, { inline_keyboard: btns });
}

// ═══════════════════════════════════════════════════════════════════════════
//  Utility Functions
// ═══════════════════════════════════════════════════════════════════════════

async function applyWarning(env, chatId, userId, firstName, settings) {
  if (!settings.warnings_enabled) return '';
  let warns = await getWarnings(env, chatId, userId);
  warns++;
  await setWarnings(env, chatId, userId, warns);
  if (warns >= settings.max_warnings) {
    await setWarnings(env, chatId, userId, 0);
    const n = esc(firstName);
    if (settings.warn_action === 'ban')  { await banUser(env, chatId, userId);                    return '🚫 تم حظر ' + n + ' بعد ' + settings.max_warnings + ' إنذارات'; }
    if (settings.warn_action === 'kick') { await kickUser(env, chatId, userId);                   return '👢 تم طرد ' + n + ' بعد ' + settings.max_warnings + ' إنذارات'; }
    if (settings.warn_action === 'mute') { await restrictUser(env, chatId, userId, false, 86400); return '🔇 تم كتم ' + n + ' بعد ' + settings.max_warnings + ' إنذارات'; }
  }
  return '⚠️ إنذار ' + warns + '/' + settings.max_warnings + ' لـ ' + esc(firstName);
}

function hasLink(text) {
  if (!text) return false;
  return /https?:\/\/|t\.me\/|@[a-zA-Z]/i.test(text);
}

function esc(text) {
  return (text || '').replace(/&/g, 'and').replace(/</g, '(').replace(/>/g, ')');
}

// تحويل نص الوقت إلى ثواني: 30m / 2h / 1d
function parseTime(str) {
  if (!str) return 0;
  const m = str.match(/^(\d+)(s|m|h|d)$/i);
  if (!m) return 0;
  const n = parseInt(m[1]);
  const u = m[2].toLowerCase();
  if (u === 's') return n;
  if (u === 'm') return n * 60;
  if (u === 'h') return n * 3600;
  if (u === 'd') return n * 86400;
  return 0;
}

// توليد معادلة رياضية عشوائية للكابتشا
function rand(min, max) { return Math.floor(Math.random() * (max - min + 1)) + min; }
function genMath() {
  const ops = ['+', '-', '×'];
  const op  = ops[Math.floor(Math.random() * 3)];
  let a, b, ans;
  if (op === '+') { a = rand(2, 15); b = rand(2, 15); ans = a + b; }
  if (op === '-') { a = rand(6, 20); b = rand(1, a - 1); ans = a - b; }
  if (op === '×') { a = rand(2, 9);  b = rand(2, 9);  ans = a * b; }
  const wrongs = new Set();
  while (wrongs.size < 2) {
    const w = ans + (Math.random() < 0.5 ? 1 : -1) * rand(1, 5);
    if (w !== ans && w > 0) wrongs.add(w);
  }
  const opts = [ans, ...[...wrongs]].sort(() => Math.random() - 0.5);
  return { question: a + ' ' + op + ' ' + b + ' = ؟', answer: ans, opts };
}

// إرسال حدث لقناة السجل
async function sendLog(env, settings, text) {
  if (!settings || !settings.log_channel) return;
  try { await sendMsg(env, parseInt(settings.log_channel), text); } catch {}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Telegram API Wrappers
// ═══════════════════════════════════════════════════════════════════════════

async function tgPost(env, method, body) {
  const r = await fetch(API(env) + '/' + method, {
    method:  'POST',
    headers: { 'Content-Type': 'application/json' },
    body:    JSON.stringify(body),
  });
  return (await r.json()).result;
}

async function sendMsg(env, chatId, text, keyboard) {
  const body = { chat_id: chatId, text };
  if (keyboard) body.reply_markup = keyboard;
  return tgPost(env, 'sendMessage', body);
}

async function sendPhoto(env, chatId, fileId, caption, keyboard) {
  const body = { chat_id: chatId, photo: fileId, caption: caption || '' };
  if (keyboard) body.reply_markup = keyboard;
  return tgPost(env, 'sendPhoto', body);
}

async function editMsg(env, chatId, msgId, text, keyboard) {
  const body = { chat_id: chatId, message_id: msgId, text };
  if (keyboard) body.reply_markup = keyboard;
  return tgPost(env, 'editMessageText', body);
}

async function deleteMsg(env, chatId, msgId) {
  return tgPost(env, 'deleteMessage', { chat_id: chatId, message_id: msgId });
}

async function pinMessage(env, chatId, msgId) {
  return tgPost(env, 'pinChatMessage', { chat_id: chatId, message_id: msgId, disable_notification: false });
}

async function unpinMessage(env, chatId, msgId) {
  return tgPost(env, 'unpinChatMessage', { chat_id: chatId, message_id: msgId });
}

async function answerCb(env, id, text) {
  const body = { callback_query_id: id };
  if (text) body.text = text;
  return tgPost(env, 'answerCallbackQuery', body);
}

async function getUserByUsername(env, username) {
  try {
    const r = await fetch(API(env) + '/getChat', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ chat_id: '@' + username }) });
    const d = await r.json();
    return d.ok ? d.result : null;
  } catch { return null; }
}

async function getChatMember(env, chatId, userId) {
  const r = await fetch(API(env) + '/getChatMember', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ chat_id: chatId, user_id: userId }) });
  const d = await r.json();
  return d.ok ? d.result : null;
}

async function banUser(env, chatId, userId, durationSecs) {
  const body = { chat_id: chatId, user_id: userId };
  if (durationSecs) body.until_date = Math.floor(Date.now() / 1000) + durationSecs;
  return tgPost(env, 'banChatMember', body);
}

async function unbanUser(env, chatId, userId) {
  return tgPost(env, 'unbanChatMember', { chat_id: chatId, user_id: userId, only_if_banned: true });
}

async function kickUser(env, chatId, userId) {
  await banUser(env, chatId, userId);
  await unbanUser(env, chatId, userId);
}

async function restrictUser(env, chatId, userId, canSend, duration) {
  const until = duration ? Math.floor(Date.now() / 1000) + duration : 0;
  const perms = {
    can_send_messages:         canSend,
    can_send_audios:           canSend,
    can_send_documents:        canSend,
    can_send_photos:           canSend,
    can_send_videos:           canSend,
    can_send_video_notes:      canSend,
    can_send_voice_notes:      canSend,
    can_send_polls:            canSend,
    can_send_other_messages:   canSend,
    can_add_web_page_previews: canSend,
    can_change_info:           false,
    can_invite_users:          canSend,
    can_pin_messages:          false,
  };
  return tgPost(env, 'restrictChatMember', { chat_id: chatId, user_id: userId, permissions: perms, until_date: until });
}

async function setChatPermissions(env, chatId, canSend) {
  const perms = {
    can_send_messages:         canSend,
    can_send_audios:           canSend,
    can_send_documents:        canSend,
    can_send_photos:           canSend,
    can_send_videos:           canSend,
    can_send_video_notes:      canSend,
    can_send_voice_notes:      canSend,
    can_send_polls:            canSend,
    can_send_other_messages:   canSend,
    can_add_web_page_previews: canSend,
    can_invite_users:          true,
    can_pin_messages:          false,
    can_change_info:           false,
  };
  return tgPost(env, 'setChatPermissions', { chat_id: chatId, permissions: perms, use_independent_chat_permissions: true });
}

async function applyMediaPermissions(env, chatId, settings) {
  if (settings.group_locked) return;
  const ml = settings.media_lock;
  const perms = {
    can_send_messages:         true,
    can_send_photos:           !ml.photo,
    can_send_videos:           !ml.video,
    can_send_video_notes:      !ml.video,
    can_send_audios:           !ml.audio,
    can_send_voice_notes:      !ml.mic,
    can_send_documents:        !ml.document,
    can_send_polls:            !ml.poll,
    can_send_other_messages:   !(ml.sticker || ml.gif),
    can_add_web_page_previews: true,
    can_invite_users:          true,
    can_pin_messages:          false,
    can_change_info:           false,
  };
  return tgPost(env, 'setChatPermissions', { chat_id: chatId, permissions: perms, use_independent_chat_permissions: true });
}

async function setChatSlowMode(env, chatId, seconds) {
  return tgPost(env, 'setChatSlowModeDelay', { chat_id: chatId, seconds });
}
