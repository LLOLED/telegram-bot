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

const API   = (env) => `https://api.telegram.org/bot${env.BOT_TOKEN}`;
const TTL   = 60 * 60 * 24 * 365; // 1 year

// ─── Default Settings ────────────────────────────────────────────────────────

function defaultSettings() {
  return {
    // قفل عام للمجموعة
    group_locked: false,

    // الحماية
    links_locked:  false,
    anti_forward:  false,
    anti_channel:  false,

    // قفل الوسائط (كل نوع بشكل مستقل)
    media_lock: {
      photo:    false,
      video:    false,
      sticker:  false,
      audio:    false,
      mic:      false,
      gif:      false,
      document: false,
      poll:     false,
    },

    // الإنذارات والعقوبات
    warnings_enabled: true,
    max_warnings:     3,
    warn_action:      'kick', // kick | ban | mute

    // الكلمات المحظورة
    banned_words: [],

    // الردود التلقائية
    auto_replies: {},

    // الترحيب والكابتشا
    welcome_enabled: false,
    welcome_message: 'أهلاً {name} في {group} 🎉',
    captcha_enabled: false,

    // الوضع الليلي
    night_mode: {
      enabled: false,
      start:   23,
      end:     6,
    },

    // إعدادات الكتم
    default_mute_duration: 3600, // بالثواني

    // المشرفون الفرعيون (مصفوفة من strings)
    sub_admins: [],
  };
}

// ─── KV Helpers ──────────────────────────────────────────────────────────────

async function getSettings(env, chatId) {
  try {
    const raw = await env.GROUP_SETTINGS.get('s_' + chatId);
    if (!raw) return defaultSettings();
    const saved = JSON.parse(raw);
    const def   = defaultSettings();
    return {
      ...def,
      ...saved,
      media_lock: { ...def.media_lock, ...(saved.media_lock || {}) },
      night_mode: { ...def.night_mode, ...(saved.night_mode || {}) },
      sub_admins: saved.sub_admins || [],
    };
  } catch {
    return defaultSettings();
  }
}

async function saveSettings(env, chatId, s) {
  try {
    await env.GROUP_SETTINGS.put('s_' + chatId, JSON.stringify(s), { expirationTtl: TTL });
  } catch {}
}

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

// ─── Register Admins (for /start group detection) ────────────────────────────

async function registerAdmins(env, chatId, chatTitle) {
  try {
    const r = await fetch(API(env) + '/getChatAdministrators', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: chatId }),
    });
    const d = await r.json();
    if (d.ok) {
      for (const admin of d.result) {
        if (!admin.user.is_bot) {
          await addUserGroup(env, admin.user.id, chatId, chatTitle);
        }
      }
    }
  } catch {}
}

// ─── Main Processor ───────────────────────────────────────────────────────────

async function processUpdate(update, env) {
  if (update.message)        await handleMessage(update.message, env);
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

  // ── محادثة خاصة (Private) ───────────────────────────────────────────────
  if (chatType === 'private') {
    const pending = await getPending(env, userId);

    if (pending) {

      // ── انتظار كلمة محظورة ─────────────────────────────────────────────
      if (pending.step === 'await_banned_word') {
        const gChatId = pending.gChatId;
        if (!await isAuthorized(env, gChatId, userId)) {
          await setPending(env, userId, null);
          await sendMsg(env, chatId, '❌ انتهت صلاحيتك.');
          return;
        }
        const word = text.trim();
        if (!word) {
          await sendMsg(env, chatId, 'أرسل الكلمة المراد حظرها:');
          return;
        }
        const settings = await getSettings(env, gChatId);
        if (settings.banned_words.includes(word)) {
          await setPending(env, userId, null);
          await sendMsg(env, chatId, '⚠️ هذه الكلمة موجودة مسبقاً.',
            { inline_keyboard: [[{ text: '🔙 رجوع', callback_data: 'words_' + gChatId }]] }
          );
          return;
        }
        settings.banned_words.push(word);
        await saveSettings(env, gChatId, settings);
        await setPending(env, userId, null);
        await sendMsg(env, chatId, '✅ تمت إضافة الكلمة المحظورة: "' + word + '"',
          { inline_keyboard: [
            [{ text: '🚫 إدارة الكلمات المحظورة', callback_data: 'words_' + gChatId }],
            [{ text: '🔙 رجوع للقائمة', callback_data: 'menu_' + gChatId }],
          ]}
        );
        return;
      }

      // ── انتظار رسالة الترحيب ───────────────────────────────────────────
      if (pending.step === 'await_welcome_msg') {
        const gChatId = pending.gChatId;
        if (!await isAuthorized(env, gChatId, userId)) {
          await setPending(env, userId, null);
          await sendMsg(env, chatId, '❌ انتهت صلاحيتك.');
          return;
        }
        const newMsg = text.trim();
        if (!newMsg) {
          await sendMsg(env, chatId, 'أرسل نص رسالة الترحيب:\n\n💡 استخدم {name} لاسم العضو و {group} لاسم المجموعة');
          return;
        }
        const settings = await getSettings(env, gChatId);
        settings.welcome_message = newMsg;
        await saveSettings(env, gChatId, settings);
        await setPending(env, userId, null);
        await sendMsg(env, chatId,
          '✅ تم تحديث رسالة الترحيب!\n\nالرسالة الجديدة:\n' + esc(newMsg) + '\n\n💡 يمكنك استخدام {name} لاسم العضو و {group} لاسم المجموعة',
          { inline_keyboard: [
            [{ text: '👋 إعدادات الترحيب', callback_data: 'welcome_' + gChatId }],
            [{ text: '🔙 رجوع للقائمة', callback_data: 'menu_' + gChatId }],
          ]}
        );
        return;
      }

      // ── انتظار ساعة بداية الوضع الليلي ───────────────────────────────
      if (pending.step === 'await_night_start') {
        const gChatId = pending.gChatId;
        if (!await isAuthorized(env, gChatId, userId)) {
          await setPending(env, userId, null);
          await sendMsg(env, chatId, '❌ انتهت صلاحيتك.');
          return;
        }
        const hour = parseInt(text.trim());
        if (isNaN(hour) || hour < 0 || hour > 23) {
          await sendMsg(env, chatId, '❌ أرسل رقم صحيح بين 0 و 23:');
          return;
        }
        await setPending(env, userId, { step: 'await_night_end', gChatId, night_start: hour });
        await sendMsg(env, chatId, '✅ ساعة البداية: ' + hour + ':00\n\nالآن أرسل ساعة الانتهاء (0-23):');
        return;
      }

      // ── انتظار ساعة نهاية الوضع الليلي ───────────────────────────────
      if (pending.step === 'await_night_end') {
        const gChatId = pending.gChatId;
        if (!await isAuthorized(env, gChatId, userId)) {
          await setPending(env, userId, null);
          await sendMsg(env, chatId, '❌ انتهت صلاحيتك.');
          return;
        }
        const hour = parseInt(text.trim());
        if (isNaN(hour) || hour < 0 || hour > 23) {
          await sendMsg(env, chatId, '❌ أرسل رقم صحيح بين 0 و 23:');
          return;
        }
        const settings = await getSettings(env, gChatId);
        settings.night_mode.start = pending.night_start;
        settings.night_mode.end   = hour;
        await saveSettings(env, gChatId, settings);
        await setPending(env, userId, null);
        await sendMsg(env, chatId,
          '✅ تم تحديث وقت الوضع الليلي!\n\nمن الساعة ' + pending.night_start + ':00 حتى ' + hour + ':00',
          { inline_keyboard: [
            [{ text: '🌙 إعدادات الوضع الليلي', callback_data: 'night_' + gChatId }],
            [{ text: '🔙 رجوع للقائمة', callback_data: 'menu_' + gChatId }],
          ]}
        );
        return;
      }

      // ── انتظار @username لإضافة مشرف فرعي ───────────────────────────
      if (pending.step === 'await_sub_admin_username') {
        const gChatId = pending.gChatId;
        if (!await isOwner(env, gChatId, userId)) {
          await setPending(env, userId, null);
          await sendMsg(env, chatId, '❌ هذه الخاصية للمالك فقط.');
          return;
        }

        const input    = text.trim().replace(/^@/, '');
        let subId      = null;
        let subName    = null;
        let subUsername = null;

        if (!input) {
          await sendMsg(env, chatId, '❌ أرسل اسم المستخدم بشكل صحيح مثال: @username');
          return;
        }

        // البحث عن المستخدم عبر getChat بالـ @username
        const userInfo = await getUserByUsername(env, input);
        if (!userInfo) {
          await sendMsg(env, chatId,
            '❌ لم يُعثر على المستخدم @' + input + '\n\nتأكد من صحة الـ username وأنه مسجّل في تيليغرام.',
            { inline_keyboard: [[{ text: '❌ إلغاء', callback_data: 'subadmins_' + gChatId }]] }
          );
          return;
        }

        subId       = userInfo.id.toString();
        subName     = userInfo.first_name || input;
        subUsername = userInfo.username   || input;

        // التحقق أن الشخص موجود في المجموعة
        const memberInfo = await getChatMember(env, parseInt(gChatId), userInfo.id);
        if (!memberInfo || memberInfo.status === 'left' || memberInfo.status === 'kicked') {
          await sendMsg(env, chatId,
            '❌ المستخدم @' + subUsername + ' ليس عضواً في المجموعة.\n\nيجب أن يكون موجوداً في المجموعة أولاً.',
            { inline_keyboard: [[{ text: '❌ إلغاء', callback_data: 'subadmins_' + gChatId }]] }
          );
          return;
        }

        const settings = await getSettings(env, gChatId);
        if (!settings.sub_admins.includes(subId)) {
          settings.sub_admins.push(subId);
          await saveSettings(env, gChatId, settings);
          await addUserGroup(env, userInfo.id, gChatId, pending.groupName || gChatId);
        }
        await setPending(env, userId, null);
        await sendMsg(env, chatId,
          '✅ تمت إضافة المشرف الفرعي!\n\nالاسم: ' + esc(subName) + '\nالمعرّف: @' + subUsername + '\nID: ' + subId,
          { inline_keyboard: [
            [{ text: '👮 إدارة المشرفين الفرعيين', callback_data: 'subadmins_' + gChatId }],
            [{ text: '🔙 رجوع للقائمة',            callback_data: 'menu_'       + gChatId }],
          ]}
        );
        return;
      }

      // ── انتظار محتوى الإعلان ──────────────────────────────────────────
      if (pending.step === 'await_ann_content') {
        const gChatId = pending.gChatId;
        if (!await isAuthorized(env, gChatId, userId)) {
          await setPending(env, userId, null);
          await sendMsg(env, chatId, '❌ أنت لست مشرفاً في هذه المجموعة بعد الآن.');
          return;
        }
        let photoFileId = null;
        let annText = msg.caption || msg.text || '';
        if (msg.photo && msg.photo.length > 0) {
          photoFileId = msg.photo[msg.photo.length - 1].file_id;
        }
        if (!annText && !photoFileId) {
          await sendMsg(env, chatId, '❌ الرجاء إرسال نص أو صورة (مع نص اختياري).');
          return;
        }
        await setPending(env, userId, {
          step:         'confirm_ann',
          gChatId,
          annText,
          photoFileId:  photoFileId || null,
          orig_msg_id:  pending.orig_msg_id,
          orig_chat_id: pending.orig_chat_id,
        });
        const previewCaption = '📋 معاينة الإعلان:\n\n' + (annText || '(بدون نص)') + '\n\nهل تريد إرساله وتثبيته في المجموعة؟';
        const kb = { inline_keyboard: [
          [{ text: '📌 إرسال وتثبيت', callback_data: 'ann_confirm_' + gChatId }],
          [{ text: '❌ إلغاء',         callback_data: 'ann_cancel_'  + gChatId }],
        ]};
        if (photoFileId) {
          await sendPhoto(env, chatId, photoFileId, previewCaption, kb);
        } else {
          await sendMsg(env, chatId, previewCaption, kb);
        }
        return;
      }
    }

    // ── /start ────────────────────────────────────────────────────────────
    if (text === '/start') {
      await sendMsg(env, chatId,
        'أهلاً بك في بوت إدارة المجموعات 🤖\n\nأضف البوت لمجموعتك كمشرف ثم اضغط زر مجموعاتي',
        { inline_keyboard: [[{ text: '👥 مجموعاتي', callback_data: 'my_groups' }]] }
      );
    }
    return;
  }

  // ── مجموعات فقط ───────────────────────────────────────────────────────────
  if (chatType !== 'group' && chatType !== 'supergroup') return;

  const member  = await getChatMember(env, chatId, userId);
  const isAdmin = member && (member.status === 'creator' || member.status === 'administrator');

  // تسجيل المشرفين تلقائياً
  await registerAdmins(env, chatId, msg.chat.title || '');

  const settings = await getSettings(env, chatId);

  // ── أوامر قفل/فتح المجموعة (المشرف فقط) ─────────────────────────────────
  if (isAdmin && (text === 'قفل' || text === 'فتح')) {
    if (text === 'قفل') {
      settings.group_locked = true;
      await saveSettings(env, chatId, settings);
      await setChatPermissions(env, chatId, false);
      await sendMsg(env, chatId, '🔒 تم قفل المجموعة\nلا يمكن لأي عضو الكتابة الآن.\nأرسل "فتح" لفتحها.');
    } else {
      settings.group_locked = false;
      await saveSettings(env, chatId, settings);
      await setChatPermissions(env, chatId, true);
      await sendMsg(env, chatId, '🔓 تم فتح المجموعة\nيمكن للأعضاء الكتابة الآن.');
    }
    return;
  }

  // ── استقبال أعضاء جدد ────────────────────────────────────────────────────
  if (msg.new_chat_members) {
    for (const m of msg.new_chat_members) {
      if (m.is_bot) continue;
      if (settings.captcha_enabled) {
        await restrictUser(env, chatId, m.id, false);
        await sendMsg(env, chatId,
          'مرحباً ' + esc(m.first_name) + ' 👋\nاضغط الزر خلال 5 دقائق للتحقق من أنك لست روبوت',
          { inline_keyboard: [[{ text: '✅ لست روبوت', callback_data: 'cap_' + m.id + '_' + chatId }]] }
        );
      } else if (settings.welcome_enabled) {
        const wmsg = settings.welcome_message
          .replace('{name}',  m.first_name)
          .replace('{group}', msg.chat.title || '');
        await sendMsg(env, chatId, wmsg);
      }
    }
    return;
  }

  // ── قفل المجموعة — حذف رسائل الأعضاء ────────────────────────────────────
  if (!isAdmin && settings.group_locked) {
    await deleteMsg(env, chatId, msgId);
    return;
  }

  // ── فحوصات على رسائل الأعضاء العاديين ───────────────────────────────────
  if (!isAdmin) {
    const name = esc(msg.from.first_name);

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

    // ── قفل الوسائط — حذف صامت (Telegram يمنع الإرسال مسبقاً عبر setChatPermissions)
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
    if (settings.anti_channel && msg.sender_chat) {
      await deleteMsg(env, chatId, msgId);
      return;
    }

    // الوضع الليلي
    if (settings.night_mode.enabled) {
      const h  = (new Date().getUTCHours() + 3) % 24;
      const st = settings.night_mode.start;
      const en = settings.night_mode.end;
      const isNight = st > en ? (h >= st || h < en) : (h >= st && h < en);
      if (isNight) {
        await deleteMsg(env, chatId, msgId);
        await sendMsg(env, chatId, name + ' 🌙 المجموعة في الوضع الليلي');
        return;
      }
    }
  }

  // ── الردود التلقائية (للجميع) ────────────────────────────────────────────
  for (const [kw, reply] of Object.entries(settings.auto_replies)) {
    if (text.toLowerCase().includes(kw.toLowerCase())) {
      await sendMsg(env, chatId, reply);
      return;
    }
  }

  // ── أوامر المشرف (/ban /kick /mute ...) ──────────────────────────────────
  if (isAdmin && text.startsWith('/')) {
    const parts = text.split(' ');
    const cmd   = parts[0].split('@')[0];
    const reply = msg.reply_to_message;

    if (cmd === '/ban' && reply) {
      await banUser(env, chatId, reply.from.id);
      await sendMsg(env, chatId, '🚫 تم حظر ' + esc(reply.from.first_name));

    } else if (cmd === '/unban' && reply) {
      await unbanUser(env, chatId, reply.from.id);
      await sendMsg(env, chatId, '✅ تم فك حظر ' + esc(reply.from.first_name));

    } else if (cmd === '/kick' && reply) {
      await kickUser(env, chatId, reply.from.id);
      await sendMsg(env, chatId, '👢 تم طرد ' + esc(reply.from.first_name));

    } else if (cmd === '/mute' && reply) {
      const customMins = parseInt(parts[1]);
      const durSecs    = customMins ? customMins * 60 : (settings.default_mute_duration || 3600);
      const durMins    = Math.round(durSecs / 60);
      await restrictUser(env, chatId, reply.from.id, false, durSecs);
      await sendMsg(env, chatId, '🔇 تم كتم ' + esc(reply.from.first_name) + ' لمدة ' + durMins + ' دقيقة');

    } else if (cmd === '/unmute' && reply) {
      await restrictUser(env, chatId, reply.from.id, true);
      await sendMsg(env, chatId, '🔊 تم فك كتم ' + esc(reply.from.first_name));

    } else if (cmd === '/warn' && reply) {
      const result = await applyWarning(env, chatId, reply.from.id, reply.from.first_name, settings);
      await sendMsg(env, chatId, result);

    } else if (cmd === '/resetwarn' && reply) {
      await setWarnings(env, chatId, reply.from.id, 0);
      await sendMsg(env, chatId, '✅ تم مسح إنذارات ' + esc(reply.from.first_name));

    } else if (cmd === '/addreply') {
      const kw    = parts[1];
      const rtext = parts.slice(2).join(' ');
      if (kw && rtext) {
        settings.auto_replies[kw] = rtext;
        await saveSettings(env, chatId, settings);
        await sendMsg(env, chatId, '✅ تم إضافة الرد التلقائي للكلمة: ' + kw);
      }

    } else if (cmd === '/removereply') {
      const kw = parts[1];
      if (kw) {
        delete settings.auto_replies[kw];
        await saveSettings(env, chatId, settings);
        await sendMsg(env, chatId, '✅ تم حذف الرد التلقائي للكلمة: ' + kw);
      }
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

  // ── كابتشا (يعمل في المجموعة) ────────────────────────────────────────────
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
    if (!await isAuthorized(env, gChatId, userId)) {
      await setPending(env, userId, null);
      await editMsg(env, chatId, msgId, '❌ أنت لست مشرفاً في هذه المجموعة بعد الآن.');
      return;
    }
    let sentMsg = null;
    if (pending.photoFileId) {
      sentMsg = await sendPhoto(env, parseInt(gChatId), pending.photoFileId, pending.annText || '');
    } else {
      sentMsg = await sendMsg(env, parseInt(gChatId), pending.annText);
    }
    if (sentMsg && sentMsg.message_id) {
      await pinMessage(env, parseInt(gChatId), sentMsg.message_id);
      const anns = await getAnnouncements(env, gChatId);
      anns.push({
        id:       sentMsg.message_id,
        text:     (pending.annText || '').substring(0, 80),
        hasPhoto: !!pending.photoFileId,
        date:     new Date().toISOString(),
      });
      await saveAnnouncements(env, gChatId, anns);
      await setPending(env, userId, null);
      await editMsg(env, chatId, msgId,
        '✅ تم إرسال الإعلان وتثبيته في المجموعة بنجاح!',
        { inline_keyboard: [
          [{ text: '📌 إعلان جديد',       callback_data: 'announce_'  + gChatId }],
          [{ text: '🗑️ إدارة الإعلانات', callback_data: 'ann_list_'  + gChatId }],
          [{ text: '🔙 رجوع للقائمة',     callback_data: 'menu_'      + gChatId }],
        ]}
      );
    } else {
      await setPending(env, userId, null);
      await editMsg(env, chatId, msgId,
        '❌ حدث خطأ أثناء الإرسال. تأكد أن البوت مشرف في المجموعة.',
        { inline_keyboard: [[{ text: '🔙 رجوع للقائمة', callback_data: 'menu_' + gChatId }]] }
      );
    }
    return;
  }

  // ── إلغاء الإعلان ─────────────────────────────────────────────────────────
  if (data.startsWith('ann_cancel_')) {
    const gChatId = data.replace('ann_cancel_', '');
    await setPending(env, userId, null);
    await editMsg(env, chatId, msgId, '❌ تم إلغاء الإعلان.',
      { inline_keyboard: [[{ text: '🔙 رجوع للقائمة', callback_data: 'menu_' + gChatId }]] }
    );
    return;
  }

  // ── قائمة الإعلانات ───────────────────────────────────────────────────────
  if (data.startsWith('ann_list_')) {
    const gChatId = data.replace('ann_list_', '');
    if (!await isAuthorized(env, gChatId, userId)) return;
    await showAnnList(env, chatId, msgId, gChatId);
    return;
  }

  // ── حذف إعلان ─────────────────────────────────────────────────────────────
  if (data.startsWith('ann_del_')) {
    const withoutPrefix = data.replace('ann_del_', '');
    const firstUnder    = withoutPrefix.indexOf('_');
    const idx           = parseInt(withoutPrefix.substring(0, firstUnder));
    const gChatId       = withoutPrefix.substring(firstUnder + 1);
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

  // ── فتح نافذة إعلان جديد ─────────────────────────────────────────────────
  if (data.startsWith('announce_')) {
    const gChatId = data.replace('announce_', '');
    if (!await isAuthorized(env, gChatId, userId)) return;
    await setPending(env, userId, { step: 'await_ann_content', gChatId, orig_msg_id: msgId, orig_chat_id: chatId });
    await editMsg(env, chatId, msgId,
      '📌 تثبيت إعلان جديد\n\nأرسل نص الإعلان الآن، أو أرسل صورة مع نص (كـ caption).\n\nسيتم عرض معاينة للتأكيد قبل الإرسال.',
      { inline_keyboard: [[{ text: '❌ إلغاء', callback_data: 'ann_cancel_' + gChatId }]] }
    );
    return;
  }

  // ── مجموعاتي ──────────────────────────────────────────────────────────────
  if (data === 'my_groups') {
    const groups = await getUserGroups(env, userId);
    const keys   = Object.keys(groups);
    if (keys.length === 0) {
      await editMsg(env, chatId, msgId,
        'ما عندك مجموعات مسجلة\nأضف البوت لمجموعتك كمشرف وأرسل أي رسالة فيها',
        { inline_keyboard: [[{ text: '🔄 تحديث', callback_data: 'my_groups' }]] }
      );
      return;
    }
    const btns = keys.map(id => [{ text: groups[id], callback_data: 'grp_' + id }]);
    btns.push([{ text: '🔄 تحديث', callback_data: 'my_groups' }]);
    await editMsg(env, chatId, msgId, 'مجموعاتك:\nاختر مجموعة للإدارة:', { inline_keyboard: btns });
    return;
  }

  // ── اختيار مجموعة ─────────────────────────────────────────────────────────
  if (data.startsWith('grp_')) {
    const gId = data.replace('grp_', '');
    if (!await isAuthorized(env, gId, userId)) {
      await editMsg(env, chatId, msgId, '❌ أنت لست مشرفاً في هذه المجموعة');
      return;
    }
    await showMainMenu(env, chatId, msgId, gId, userId);
    return;
  }

  // ── استخراج gChatId من نهاية data ─────────────────────────────────────────
  const lastUnder = data.lastIndexOf('_');
  const gChatId   = lastUnder !== -1 ? data.substring(lastUnder + 1) : '';

  if (!await isAuthorized(env, gChatId, userId)) return;

  const settings = await getSettings(env, gChatId);

  // ── Routing لصفحات القوائم ────────────────────────────────────────────────
  if (data === 'menu_'      + gChatId) { await setPending(env, userId, null); await showMainMenu(env, chatId, msgId, gChatId, userId);           return; }
  if (data === 'welcome_'   + gChatId) { await setPending(env, userId, null); await showWelcomeMenu(env, chatId, msgId, gChatId, settings);       return; }
  if (data === 'protect_'   + gChatId) { await setPending(env, userId, null); await showProtectMenu(env, chatId, msgId, gChatId, settings);       return; }
  if (data === 'media_'     + gChatId) { await setPending(env, userId, null); await showMediaMenu(env, chatId, msgId, gChatId, settings);         return; }
  if (data === 'warns_'     + gChatId) { await setPending(env, userId, null); await showWarnsMenu(env, chatId, msgId, gChatId, settings);         return; }
  if (data === 'night_'     + gChatId) { await setPending(env, userId, null); await showNightMenu(env, chatId, msgId, gChatId, settings);         return; }
  if (data === 'replies_'   + gChatId) { await setPending(env, userId, null); await showRepliesMenu(env, chatId, msgId, gChatId, settings);       return; }
  if (data === 'words_'     + gChatId) { await setPending(env, userId, null); await showWordsMenu(env, chatId, msgId, gChatId, settings);         return; }
  if (data === 'subadmins_' + gChatId) { await setPending(env, userId, null); await showSubAdminsMenu(env, chatId, msgId, gChatId, settings, userId); return; }
  if (data === 'mute_menu_' + gChatId) { await setPending(env, userId, null); await showMuteMenu(env, chatId, msgId, gChatId, settings);          return; }

  // ── إضافة كلمة محظورة ────────────────────────────────────────────────────
  if (data === 'add_word_' + gChatId) {
    await setPending(env, userId, { step: 'await_banned_word', gChatId });
    await editMsg(env, chatId, msgId,
      '🚫 إضافة كلمة محظورة\n\nأرسل الكلمة التي تريد حظرها الآن:',
      { inline_keyboard: [[{ text: '❌ إلغاء', callback_data: 'words_' + gChatId }]] }
    );
    return;
  }

  // ── حذف كلمة محظورة ──────────────────────────────────────────────────────
  if (data.startsWith('del_word_')) {
    const wp         = data.replace('del_word_', '');
    const wFirstU    = wp.indexOf('_');
    const wordIdx    = parseInt(wp.substring(0, wFirstU));
    const wGChatId   = wp.substring(wFirstU + 1);
    if (!await isAuthorized(env, wGChatId, userId)) return;
    const wSettings  = await getSettings(env, wGChatId);
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
    await editMsg(env, chatId, msgId,
      '✏️ تعديل رسالة الترحيب\n\nأرسل نص رسالة الترحيب الجديدة:\n\n💡 استخدم {name} لاسم العضو\n💡 استخدم {group} لاسم المجموعة\n\nمثال: أهلاً {name} في {group} 🎉',
      { inline_keyboard: [[{ text: '❌ إلغاء', callback_data: 'welcome_' + gChatId }]] }
    );
    return;
  }

  // ── تعديل وقت الوضع الليلي ───────────────────────────────────────────────
  if (data === 'edit_night_time_' + gChatId) {
    await setPending(env, userId, { step: 'await_night_start', gChatId });
    await editMsg(env, chatId, msgId,
      '🌙 تعديل وقت الوضع الليلي\n\nأرسل ساعة البداية (0-23):\nمثال: 23 تعني 11 مساءً',
      { inline_keyboard: [[{ text: '❌ إلغاء', callback_data: 'night_' + gChatId }]] }
    );
    return;
  }

  // ── إضافة مشرف فرعي ──────────────────────────────────────────────────────
  if (data === 'add_subadmin_' + gChatId) {
    if (!await isOwner(env, gChatId, userId)) {
      await editMsg(env, chatId, msgId, '❌ هذه الخاصية للمالك فقط.',
        { inline_keyboard: [[{ text: '🔙 رجوع', callback_data: 'subadmins_' + gChatId }]] }
      );
      return;
    }
    const groups    = await getUserGroups(env, userId);
    const groupName = groups[gChatId.toString()] || gChatId;
    await setPending(env, userId, { step: 'await_sub_admin_username', gChatId, groupName });
    await editMsg(env, chatId, msgId,
      '👮 إضافة مشرف فرعي\n\nأرسل معرّف الشخص الذي تريد تعيينه:\n\nمثال: @HJKL818\n\n💡 يجب أن يكون الشخص عضواً في المجموعة',
      { inline_keyboard: [[{ text: '❌ إلغاء', callback_data: 'subadmins_' + gChatId }]] }
    );
    return;
  }

  // ── حذف مشرف فرعي ────────────────────────────────────────────────────────
  if (data.startsWith('del_subadmin_')) {
    const sp       = data.replace('del_subadmin_', '');
    const sFirstU  = sp.indexOf('_');
    const subIdx   = parseInt(sp.substring(0, sFirstU));
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
    const label  = labels[durSecs] || (durSecs / 60 + ' دقيقة');
    await editMsg(env, chatId, msgId,
      '🔇 تم كتم المستخدم لمدة ' + label + ' ✅',
      { inline_keyboard: [[{ text: '🔙 رجوع للقائمة', callback_data: 'menu_' + mGChatId }]] }
    );
    return;
  }

  // ── تعيين مدة الكتم الافتراضية ───────────────────────────────────────────
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

  // ── عدد الإنذارات ─────────────────────────────────────────────────────────
  if (data.startsWith('wmax_')) {
    const num = parseInt(data.split('_')[1]);
    if (!isNaN(num)) {
      settings.max_warnings = num;
      await saveSettings(env, gChatId, settings);
      await showWarnsMenu(env, chatId, msgId, gChatId, settings);
    }
    return;
  }

  // ── إجراء الإنذار ─────────────────────────────────────────────────────────
  if (data.startsWith('wact_')) {
    const action = data.split('_')[1];
    if (['kick', 'ban', 'mute'].includes(action)) {
      settings.warn_action = action;
      await saveSettings(env, gChatId, settings);
      await showWarnsMenu(env, chatId, msgId, gChatId, settings);
    }
    return;
  }

  // ── Toggles (تشغيل/إيقاف) ────────────────────────────────────────────────
  const toggles = {
    // الترحيب
    ['twelcome_'   + gChatId]: () => { settings.welcome_enabled      = !settings.welcome_enabled;      return () => showWelcomeMenu(env, chatId, msgId, gChatId, settings); },
    ['tcaptcha_'   + gChatId]: () => { settings.captcha_enabled      = !settings.captcha_enabled;      return () => showWelcomeMenu(env, chatId, msgId, gChatId, settings); },
    // الحماية
    ['tlinks_'     + gChatId]: () => { settings.links_locked         = !settings.links_locked;         return () => showProtectMenu(env, chatId, msgId, gChatId, settings); },
    ['tforward_'   + gChatId]: () => { settings.anti_forward         = !settings.anti_forward;         return () => showProtectMenu(env, chatId, msgId, gChatId, settings); },
    ['tchannel_'   + gChatId]: () => { settings.anti_channel         = !settings.anti_channel;         return () => showProtectMenu(env, chatId, msgId, gChatId, settings); },
    // الإنذارات
    ['twarns_'     + gChatId]: () => { settings.warnings_enabled     = !settings.warnings_enabled;     return () => showWarnsMenu(env, chatId, msgId, gChatId, settings); },
    // الوضع الليلي
    ['tnight_'     + gChatId]: () => { settings.night_mode.enabled   = !settings.night_mode.enabled;   return () => showNightMenu(env, chatId, msgId, gChatId, settings); },
    // الوسائط — كل نوع بشكل مستقل + تطبيق على صلاحيات المجموعة فوراً
    ['tphoto_'     + gChatId]: () => { settings.media_lock.photo    = !settings.media_lock.photo;    return async () => { await applyMediaPermissions(env, parseInt(gChatId), settings); await showMediaMenu(env, chatId, msgId, gChatId, settings); }; },
    ['tvideo_'     + gChatId]: () => { settings.media_lock.video    = !settings.media_lock.video;    return async () => { await applyMediaPermissions(env, parseInt(gChatId), settings); await showMediaMenu(env, chatId, msgId, gChatId, settings); }; },
    ['tsticker_'   + gChatId]: () => { settings.media_lock.sticker  = !settings.media_lock.sticker;  return async () => { await applyMediaPermissions(env, parseInt(gChatId), settings); await showMediaMenu(env, chatId, msgId, gChatId, settings); }; },
    ['taudio_'     + gChatId]: () => { settings.media_lock.audio    = !settings.media_lock.audio;    return async () => { await applyMediaPermissions(env, parseInt(gChatId), settings); await showMediaMenu(env, chatId, msgId, gChatId, settings); }; },
    ['tmic_'       + gChatId]: () => { settings.media_lock.mic      = !settings.media_lock.mic;      return async () => { await applyMediaPermissions(env, parseInt(gChatId), settings); await showMediaMenu(env, chatId, msgId, gChatId, settings); }; },
    ['tgif_'       + gChatId]: () => { settings.media_lock.gif      = !settings.media_lock.gif;      return async () => { await applyMediaPermissions(env, parseInt(gChatId), settings); await showMediaMenu(env, chatId, msgId, gChatId, settings); }; },
    ['tdoc_'       + gChatId]: () => { settings.media_lock.document = !settings.media_lock.document; return async () => { await applyMediaPermissions(env, parseInt(gChatId), settings); await showMediaMenu(env, chatId, msgId, gChatId, settings); }; },
    ['tpoll_'      + gChatId]: () => { settings.media_lock.poll     = !settings.media_lock.poll;     return async () => { await applyMediaPermissions(env, parseInt(gChatId), settings); await showMediaMenu(env, chatId, msgId, gChatId, settings); }; },
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
    '⚙️ إدارة: ' + esc(groupName) + '\nاختر القسم:',
    { inline_keyboard: [
      [{ text: '👋 الترحيب والكابتشا',    callback_data: 'welcome_'   + gChatId }],
      [{ text: '🤖 الردود التلقائية',     callback_data: 'replies_'   + gChatId }],
      [{ text: '🛡️ الحماية',             callback_data: 'protect_'   + gChatId }],
      [{ text: '🎬 الوسائط',              callback_data: 'media_'     + gChatId }],
      [{ text: '⚠️ الإنذارات والعقوبات', callback_data: 'warns_'     + gChatId }],
      [{ text: '🌙 الوضع الليلي',         callback_data: 'night_'     + gChatId }],
      [{ text: '🚫 الكلمات المحظورة',     callback_data: 'words_'     + gChatId }],
      [{ text: '🔇 إعدادات الكتم',        callback_data: 'mute_menu_' + gChatId }],
      [{ text: '👮 المشرفون الفرعيون',    callback_data: 'subadmins_' + gChatId }],
      [{ text: '📌 تثبيت إعلان',          callback_data: 'announce_'  + gChatId }],
      [{ text: '🔙 رجوع لمجموعاتي',       callback_data: 'my_groups'             }],
    ]}
  );
}

async function showWelcomeMenu(env, chatId, msgId, gChatId, s) {
  await editMsg(env, chatId, msgId,
    '👋 الترحيب والكابتشا\n\nالترحيب: ' + on(s.welcome_enabled) + '\nالكابتشا: ' + on(s.captcha_enabled) + '\n\nرسالة الترحيب الحالية:\n' + esc(s.welcome_message),
    { inline_keyboard: [
      [
        { text: on(s.welcome_enabled) + ' الترحيب',  callback_data: 'twelcome_' + gChatId },
        { text: on(s.captcha_enabled) + ' الكابتشا', callback_data: 'tcaptcha_' + gChatId },
      ],
      [{ text: '✏️ تعديل رسالة الترحيب', callback_data: 'edit_welcome_' + gChatId }],
      [{ text: '🔙 رجوع',                callback_data: 'menu_'         + gChatId }],
    ]}
  );
}

async function showProtectMenu(env, chatId, msgId, gChatId, s) {
  await editMsg(env, chatId, msgId,
    '🛡️ الحماية',
    { inline_keyboard: [
      [{ text: on(s.links_locked) + ' قفل الروابط', callback_data: 'tlinks_' + gChatId }],
      [
        { text: on(s.anti_forward) + ' منع التوجيه', callback_data: 'tforward_' + gChatId },
        { text: on(s.anti_channel) + ' منع القنوات', callback_data: 'tchannel_' + gChatId },
      ],
      [{ text: '🔙 رجوع', callback_data: 'menu_' + gChatId }],
    ]}
  );
}

async function showMediaMenu(env, chatId, msgId, gChatId, s) {
  const ml = s.media_lock;
  await editMsg(env, chatId, msgId,
    '🎬 قفل الوسائط\n\nفعّل أي نوع لمنع إرساله في المجموعة:',
    { inline_keyboard: [
      [
        { text: on(ml.photo)    + ' صور 📷',       callback_data: 'tphoto_'   + gChatId },
        { text: on(ml.video)    + ' فيديو 🎥',     callback_data: 'tvideo_'   + gChatId },
      ],
      [
        { text: on(ml.sticker)  + ' ملصقات 🗿',    callback_data: 'tsticker_' + gChatId },
        { text: on(ml.audio)    + ' صوت 🎵',       callback_data: 'taudio_'   + gChatId },
      ],
      [
        { text: on(ml.mic)      + ' مايك 🎙',      callback_data: 'tmic_'     + gChatId },
        { text: on(ml.gif)      + ' GIF 🎞',       callback_data: 'tgif_'     + gChatId },
      ],
      [
        { text: on(ml.document) + ' ملفات 📁',     callback_data: 'tdoc_'     + gChatId },
        { text: on(ml.poll)     + ' استطلاع 📊',   callback_data: 'tpoll_'    + gChatId },
      ],
      [{ text: '🔙 رجوع', callback_data: 'menu_' + gChatId }],
    ]}
  );
}

async function showWarnsMenu(env, chatId, msgId, gChatId, s) {
  const acts = { kick: 'طرد 👢', ban: 'حظر 🚫', mute: 'كتم 🔇' };
  await editMsg(env, chatId, msgId,
    '⚠️ الإنذارات والعقوبات\n\nالحالة: ' + on(s.warnings_enabled) + '\nالحد الأقصى: ' + s.max_warnings + ' إنذارات\nالعقوبة: ' + acts[s.warn_action],
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
    '🌙 الوضع الليلي\n\nالحالة: ' + on(s.night_mode.enabled) + '\nمن الساعة ' + s.night_mode.start + ':00 حتى ' + s.night_mode.end + ':00',
    { inline_keyboard: [
      [{ text: on(s.night_mode.enabled) + ' تفعيل الوضع الليلي', callback_data: 'tnight_'         + gChatId }],
      [{ text: '⏰ تعديل الوقت',                                  callback_data: 'edit_night_time_' + gChatId }],
      [{ text: '🔙 رجوع',                                         callback_data: 'menu_'            + gChatId }],
    ]}
  );
}

async function showRepliesMenu(env, chatId, msgId, gChatId, s) {
  const list = Object.keys(s.auto_replies).length > 0
    ? Object.entries(s.auto_replies).map(([k, v]) => '• ' + k + ': ' + v).join('\n')
    : 'لا توجد ردود تلقائية';
  await editMsg(env, chatId, msgId,
    '🤖 الردود التلقائية\n\n' + list + '\n\nلإضافة رد أرسل في المجموعة:\n/addreply كلمة الرد\n\nلحذف رد:\n/removereply كلمة',
    { inline_keyboard: [[{ text: '🔙 رجوع', callback_data: 'menu_' + gChatId }]] }
  );
}

async function showWordsMenu(env, chatId, msgId, gChatId, s) {
  const wordsList = s.banned_words.length > 0
    ? s.banned_words.map((w, i) => (i + 1) + '. ' + w).join('\n')
    : 'لا توجد كلمات محظورة';
  const btns = [];
  s.banned_words.forEach((word, idx) => {
    btns.push([{ text: '🗑️ حذف: ' + word, callback_data: 'del_word_' + idx + '_' + gChatId }]);
  });
  btns.push([{ text: '➕ إضافة كلمة محظورة', callback_data: 'add_word_' + gChatId }]);
  btns.push([{ text: '🔙 رجوع',             callback_data: 'menu_'     + gChatId }]);
  await editMsg(env, chatId, msgId, '🚫 الكلمات المحظورة\n\n' + wordsList, { inline_keyboard: btns });
}

async function showMuteMenu(env, chatId, msgId, gChatId, s) {
  const cur    = s.default_mute_duration || 3600;
  const labels = { 600: '10 دقائق', 1800: '30 دقيقة', 3600: 'ساعة واحدة', 43200: '12 ساعة', 86400: '24 ساعة' };
  const sel    = (v) => cur === v ? '◀ ' + labels[v] + ' ▶' : labels[v];
  await editMsg(env, chatId, msgId,
    '🔇 إعدادات الكتم\n\nالمدة الافتراضية: ' + (labels[cur] || (cur / 60 + ' دقيقة')) + '\n\nتُستخدم هذه المدة عند /mute بدون تحديد دقائق\nيمكنك تحديد مدة مخصصة: /mute 45',
    { inline_keyboard: [
      [
        { text: sel(600),   callback_data: 'mute_def_600_'   + gChatId },
        { text: sel(1800),  callback_data: 'mute_def_1800_'  + gChatId },
      ],
      [
        { text: sel(3600),  callback_data: 'mute_def_3600_'  + gChatId },
        { text: sel(43200), callback_data: 'mute_def_43200_' + gChatId },
      ],
      [
        { text: sel(86400), callback_data: 'mute_def_86400_' + gChatId },
      ],
      [{ text: '🔙 رجوع', callback_data: 'menu_' + gChatId }],
    ]}
  );
}

async function showSubAdminsMenu(env, chatId, msgId, gChatId, s, userId) {
  const ownerCheck = await isOwner(env, gChatId, userId);
  const btns       = [];
  let bodyText     = '👮 المشرفون الفرعيون\n\n';
  if (s.sub_admins.length === 0) {
    bodyText += 'لا يوجد مشرفون فرعيون حالياً.';
  } else {
    s.sub_admins.forEach((id, idx) => {
      bodyText += (idx + 1) + '. ID: ' + id + '\n';
      if (ownerCheck) {
        btns.push([{ text: '🗑️ حذف المشرف ' + (idx + 1), callback_data: 'del_subadmin_' + idx + '_' + gChatId }]);
      }
    });
  }
  if (ownerCheck) {
    bodyText += '\n💡 المشرف الفرعي يمكنه إدارة إعدادات البوت فقط (لا يملك صلاحيات تيليغرام)';
    btns.push([{ text: '➕ إضافة مشرف فرعي', callback_data: 'add_subadmin_' + gChatId }]);
  } else {
    bodyText += '\n⚠️ إضافة وحذف المشرفين الفرعيين للمالك فقط';
  }
  btns.push([{ text: '🔙 رجوع', callback_data: 'menu_' + gChatId }]);
  await editMsg(env, chatId, msgId, bodyText, { inline_keyboard: btns });
}

async function showAnnList(env, chatId, msgId, gChatId, notice) {
  const anns = await getAnnouncements(env, gChatId);
  let bodyText = notice ? notice + '\n\n' : '';
  bodyText += '📌 الإعلانات المثبتة\n\n';
  const btns = [];
  if (anns.length === 0) {
    bodyText += 'لا توجد إعلانات مسجلة حتى الآن.';
  } else {
    anns.forEach((a, i) => {
      const icon    = a.hasPhoto ? '🖼️' : '📝';
      const preview = a.text ? a.text.substring(0, 40) + (a.text.length > 40 ? '...' : '') : '(بدون نص)';
      bodyText += (i + 1) + '. ' + icon + ' ' + preview + '\n';
      btns.push([{ text: '🗑️ حذف الإعلان ' + (i + 1), callback_data: 'ann_del_' + i + '_' + gChatId }]);
    });
  }
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
    if (settings.warn_action === 'ban')  { await banUser(env, chatId, userId);                          return '🚫 تم حظر ' + n + ' بعد ' + settings.max_warnings + ' إنذارات'; }
    if (settings.warn_action === 'kick') { await kickUser(env, chatId, userId);                         return '👢 تم طرد ' + n + ' بعد ' + settings.max_warnings + ' إنذارات'; }
    if (settings.warn_action === 'mute') { await restrictUser(env, chatId, userId, false, 86400);       return '🔇 تم كتم ' + n + ' بعد ' + settings.max_warnings + ' إنذارات'; }
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

async function answerCb(env, id) {
  return tgPost(env, 'answerCallbackQuery', { callback_query_id: id });
}

async function getUserByUsername(env, username) {
  try {
    const r = await fetch(API(env) + '/getChat', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ chat_id: '@' + username }),
    });
    const d = await r.json();
    return d.ok ? d.result : null;
  } catch { return null; }
}

async function getChatMember(env, chatId, userId) {
  const r = await fetch(API(env) + '/getChatMember', {
    method:  'POST',
    headers: { 'Content-Type': 'application/json' },
    body:    JSON.stringify({ chat_id: chatId, user_id: userId }),
  });
  const d = await r.json();
  return d.ok ? d.result : null;
}

async function banUser(env, chatId, userId) {
  return tgPost(env, 'banChatMember', { chat_id: chatId, user_id: userId });
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
  // استخدام صلاحيات Bot API 6.0+ الجديدة (كل نوع وسيلة منفصل)
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
  return tgPost(env, 'setChatPermissions', {
    chat_id: chatId,
    permissions: perms,
    use_independent_chat_permissions: true,
  });
}

// تطبيق قفل الوسائط على صلاحيات المجموعة مباشرة (Bot API 6.0+)
// الأعضاء لن يتمكنوا من إرسال الوسائط المحظورة من الأساس
async function applyMediaPermissions(env, chatId, settings) {
  if (settings.group_locked) return; // المجموعة مقفلة بالكامل مسبقاً
  const ml = settings.media_lock;
  const perms = {
    can_send_messages:         true,           // النصوص دائماً مسموحة
    can_send_photos:           !ml.photo,      // صور
    can_send_videos:           !ml.video,      // فيديو
    can_send_video_notes:      !ml.video,      // فيديو دائري
    can_send_audios:           !ml.audio,      // ملفات صوت
    can_send_voice_notes:      !ml.mic,        // رسائل مايك
    can_send_documents:        !ml.document,   // ملفات
    can_send_polls:            !ml.poll,       // استطلاعات
    // الملصقات والـ GIF تشتركان في نفس الصلاحية
    can_send_other_messages:   !(ml.sticker || ml.gif),
    can_add_web_page_previews: true,
    can_invite_users:          true,
    can_pin_messages:          false,
    can_change_info:           false,
  };
  return tgPost(env, 'setChatPermissions', {
    chat_id: chatId,
    permissions: perms,
    use_independent_chat_permissions: true,
  });
}
