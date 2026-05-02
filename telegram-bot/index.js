export default {
  async fetch(request, env) {
    if (request.method !== 'POST') return new Response('OK', { status: 200 });
    try {
      const update = await request.json();
      await processUpdate(update, env);
    } catch (e) { console.error(e); }
    return new Response('OK', { status: 200 });
  }
};

const API = (env) => `https://api.telegram.org/bot${env.BOT_TOKEN}`;
const TTL = 60 * 60 * 24 * 365;

function defaultSettings() {
  return {
    links_locked: false,
    group_locked: false,
    media_lock: { photo: false, video: false, sticker: false, audio: false, gif: false, document: false },
    warnings_enabled: true,
    max_warnings: 3,
    warn_action: 'kick',
    banned_words: [],
    auto_replies: {},
    welcome_enabled: false,
    welcome_message: 'اهلاً {name} في {group}',
    captcha_enabled: false,
    night_mode: { enabled: false, start: 23, end: 6 },
    anti_channel: false,
    anti_forward: false,
    sub_admins: [],
    default_mute_duration: 3600,
    slow_mode_delay: 0,
  };
}

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
      sub_admins: saved.sub_admins || [],
    };
  } catch { return defaultSettings(); }
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

// ─── صلاحيات ─────────────────────────────────────────────────────────────────

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

// ─── Main processor ───────────────────────────────────────────────────────────

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

      // إضافة كلمة محظورة
      if (pending.step === 'await_banned_word') {
        const gChatId = pending.gChatId;
        if (!await isAuthorized(env, gChatId, userId)) {
          await setPending(env, userId, null);
          await sendMsg(env, chatId, 'انتهت صلاحيتك.');
          return;
        }
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
            await sendMsg(env, chatId, '⚠️ هذه الكلمة موجودة مسبقاً.',
              { inline_keyboard: [[{ text: '🔙 رجوع', callback_data: 'words_' + gChatId }]] }
            );
          }
        } else {
          await sendMsg(env, chatId, 'أرسل الكلمة المراد حظرها:');
        }
        return;
      }

      // تعديل رسالة الترحيب
      if (pending.step === 'await_welcome_msg') {
        const gChatId = pending.gChatId;
        if (!await isAuthorized(env, gChatId, userId)) {
          await setPending(env, userId, null);
          await sendMsg(env, chatId, 'انتهت صلاحيتك.');
          return;
        }
        const newMsg = text.trim();
        if (newMsg) {
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
        } else {
          await sendMsg(env, chatId, 'أرسل نص رسالة الترحيب:\n\n💡 استخدم {name} لاسم العضو و {group} لاسم المجموعة');
        }
        return;
      }

      // الوضع الليلي - ساعة البداية
      if (pending.step === 'await_night_start') {
        const gChatId = pending.gChatId;
        if (!await isAuthorized(env, gChatId, userId)) {
          await setPending(env, userId, null);
          await sendMsg(env, chatId, 'انتهت صلاحيتك.');
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

      // الوضع الليلي - ساعة النهاية
      if (pending.step === 'await_night_end') {
        const gChatId = pending.gChatId;
        if (!await isAuthorized(env, gChatId, userId)) {
          await setPending(env, userId, null);
          await sendMsg(env, chatId, 'انتهت صلاحيتك.');
          return;
        }
        const hour = parseInt(text.trim());
        if (isNaN(hour) || hour < 0 || hour > 23) {
          await sendMsg(env, chatId, '❌ أرسل رقم صحيح بين 0 و 23:');
          return;
        }
        const settings = await getSettings(env, gChatId);
        settings.night_mode.start = pending.night_start;
        settings.night_mode.end = hour;
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

      // إضافة مشرف فرعي
      if (pending.step === 'await_sub_admin_id') {
        const gChatId = pending.gChatId;
        if (!await isOwner(env, gChatId, userId)) {
          await setPending(env, userId, null);
          await sendMsg(env, chatId, 'هذه الخاصية للمالك فقط.');
          return;
        }
        const subId = text.trim();
        if (!/^\d+$/.test(subId)) {
          await sendMsg(env, chatId, '❌ أرسل ID رقمي صحيح للمستخدم:');
          return;
        }
        const settings = await getSettings(env, gChatId);
        if (!settings.sub_admins.includes(subId)) {
          settings.sub_admins.push(subId);
          await saveSettings(env, gChatId, settings);
          await addUserGroup(env, parseInt(subId), gChatId, pending.groupName || gChatId);
        }
        await setPending(env, userId, null);
        await sendMsg(env, chatId,
          '✅ تمت إضافة المشرف الفرعي!\n\nID: ' + subId + '\n\nسيتمكن من إدارة إعدادات البوت في المجموعة.',
          { inline_keyboard: [
            [{ text: '👮 إدارة المشرفين الفرعيين', callback_data: 'subadmins_' + gChatId }],
            [{ text: '🔙 رجوع للقائمة', callback_data: 'menu_' + gChatId }],
          ]}
        );
        return;
      }

      // إرسال إعلان
      if (pending.step === 'await_ann_content') {
        const gChatId = pending.gChatId;
        if (!await isAuthorized(env, gChatId, userId)) {
          await setPending(env, userId, null);
          await sendMsg(env, chatId, 'أنت لست مشرفاً في هذه المجموعة بعد الآن.');
          return;
        }
        let photoFileId = null;
        let annText = msg.caption || msg.text || '';
        if (msg.photo && msg.photo.length > 0) {
          photoFileId = msg.photo[msg.photo.length - 1].file_id;
        }
        if (!annText && !photoFileId) {
          await sendMsg(env, chatId, 'الرجاء إرسال نص أو صورة (مع نص اختياري) ليتم تثبيتها في المجموعة.');
          return;
        }
        await setPending(env, userId, {
          step: 'confirm_ann',
          gChatId,
          annText,
          photoFileId: photoFileId || null,
          orig_msg_id: pending.orig_msg_id,
          orig_chat_id: pending.orig_chat_id,
        });
        const previewCaption = '📋 معاينة الإعلان:\n\n' + (annText || '(بدون نص)') + '\n\nهل تريد إرساله وتثبيته في المجموعة؟';
        if (photoFileId) {
          await sendPhoto(env, chatId, photoFileId, previewCaption,
            { inline_keyboard: [
              [{ text: '📌 إرسال وتثبيت', callback_data: 'ann_confirm_' + gChatId }],
              [{ text: '❌ إلغاء', callback_data: 'ann_cancel_' + gChatId }],
            ]}
          );
        } else {
          await sendMsg(env, chatId, previewCaption,
            { inline_keyboard: [
              [{ text: '📌 إرسال وتثبيت', callback_data: 'ann_confirm_' + gChatId }],
              [{ text: '❌ إلغاء', callback_data: 'ann_cancel_' + gChatId }],
            ]}
          );
        }
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
      await sendMsg(env, chatId,
        '🔒 تم قفل المجموعة\n\nلا يمكن لأي عضو الكتابة الآن.\nأرسل "فتح" لفتحها مرة أخرى.'
      );
    } else {
      settings.group_locked = false;
      await saveSettings(env, chatId, settings);
      await setChatPermissions(env, chatId, true);
      await sendMsg(env, chatId,
        '🔓 تم فتح المجموعة\n\nيمكن للأعضاء الكتابة الآن.'
      );
    }
    return;
  }

  // ── استقبال الأعضاء الجدد ─────────────────────────────────────────────────
  if (msg.new_chat_members) {
    for (const m of msg.new_chat_members) {
      if (m.is_bot) continue;
      if (settings.captcha_enabled) {
        await restrictUser(env, chatId, m.id, false);
        await sendMsg(env, chatId,
          'مرحباً ' + esc(m.first_name) + '\nاضغط الزر خلال 5 دقائق للتحقق',
          { inline_keyboard: [[{ text: '✅ لست روبوت', callback_data: 'cap_' + m.id + '_' + chatId }]] }
        );
      } else if (settings.welcome_enabled) {
        const wmsg = settings.welcome_message
          .replace('{name}', m.first_name)
          .replace('{group}', msg.chat.title || '');
        await sendMsg(env, chatId, wmsg);
      }
    }
    return;
  }

  // ── قفل المجموعة - حذف رسائل الأعضاء ───────────────────────────────────
  if (!isAdmin && settings.group_locked) {
    await deleteMsg(env, chatId, msgId);
    return;
  }

  if (!isAdmin) {
    if (settings.links_locked && hasLink(text)) {
      await deleteMsg(env, chatId, msgId);
      const w = await applyWarning(env, chatId, userId, msg.from.first_name, settings);
      await sendMsg(env, chatId, esc(msg.from.first_name) + ' الروابط ممنوعة!\n' + w);
      return;
    }

    for (const bw of settings.banned_words) {
      if (text.toLowerCase().includes(bw.toLowerCase())) {
        await deleteMsg(env, chatId, msgId);
        const w = await applyWarning(env, chatId, userId, msg.from.first_name, settings);
        await sendMsg(env, chatId, esc(msg.from.first_name) + ' رسالتك تحتوي على كلمة محظورة!\n' + w);
        return;
      }
    }

    if (settings.media_lock.photo && msg.photo) { await deleteMsg(env, chatId, msgId); await sendMsg(env, chatId, esc(msg.from.first_name) + ' الصور ممنوعة'); return; }
    if (settings.media_lock.video && msg.video) { await deleteMsg(env, chatId, msgId); await sendMsg(env, chatId, esc(msg.from.first_name) + ' الفيديو ممنوع'); return; }
    if (settings.media_lock.sticker && msg.sticker) { await deleteMsg(env, chatId, msgId); await sendMsg(env, chatId, esc(msg.from.first_name) + ' الملصقات ممنوعة'); return; }
    if (settings.media_lock.audio && (msg.audio || msg.voice)) { await deleteMsg(env, chatId, msgId); await sendMsg(env, chatId, esc(msg.from.first_name) + ' الصوت ممنوع'); return; }
    if (settings.media_lock.gif && msg.animation) { await deleteMsg(env, chatId, msgId); await sendMsg(env, chatId, esc(msg.from.first_name) + ' GIF ممنوع'); return; }
    if (settings.media_lock.document && msg.document) { await deleteMsg(env, chatId, msgId); await sendMsg(env, chatId, esc(msg.from.first_name) + ' الملفات ممنوعة'); return; }
    if (settings.anti_forward && (msg.forward_from || msg.forward_from_chat)) { await deleteMsg(env, chatId, msgId); await sendMsg(env, chatId, esc(msg.from.first_name) + ' التوجيه ممنوع'); return; }
    if (settings.anti_channel && msg.sender_chat) { await deleteMsg(env, chatId, msgId); return; }

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
      await banUser(env, chatId, reply.from.id);
      await sendMsg(env, chatId, 'تم حظر ' + esc(reply.from.first_name));
    } else if (cmd === '/unban' && reply) {
      await unbanUser(env, chatId, reply.from.id);
      await sendMsg(env, chatId, 'تم فك حظر ' + esc(reply.from.first_name));
    } else if (cmd === '/kick' && reply) {
      await kickUser(env, chatId, reply.from.id);
      await sendMsg(env, chatId, 'تم طرد ' + esc(reply.from.first_name));
    } else if (cmd === '/mute' && reply) {
      const customMins = parseInt(parts[1]);
      const durSecs = customMins ? customMins * 60 : (settings.default_mute_duration || 3600);
      const until = await restrictUser(env, chatId, reply.from.id, false, durSecs);
      await setMuteData(env, chatId, reply.from.id, until);
      const fmt = formatMuteExpiry(durSecs);
      await sendMsg(env, reply.from.id,
        '🔇 تم كتمك في المجموعة\n⏱ المدة: ' + fmt.label + '\n🕐 ينتهي الكتم الساعة: ' + fmt.time + '\n\nاضغط الزر أدناه لمعرفة الوقت المتبقي بدقة:',
        { inline_keyboard: [[{ text: '⏱ كم باقي؟', callback_data: 'check_mute_' + chatId + '_' + reply.from.id }]] }
      ).catch(() => {});
      await sendMsg(env, chatId, '🔇 تم كتم ' + esc(reply.from.first_name) + ' لمدة ' + fmt.label);
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

  // ── عداد الكتم الحي ───────────────────────────────────────────────────────
  if (data.startsWith('check_mute_')) {
    const parts = data.replace('check_mute_', '').split('_');
    const mChatId = parts[0];
    const mUserId = parts[1];
    if (userId.toString() !== mUserId) { await answerCb(env, cb.id); return; }
    const untilTs = await getMuteUntil(env, mChatId, userId);
    const now = Math.floor(Date.now() / 1000);
    await answerCb(env, cb.id);
    if (!untilTs || untilTs <= now) {
      await editMsg(env, chatId, msgId,
        '✅ انتهى الكتم!\nيمكنك الكتابة في المجموعة الآن.'
      );
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

  // ── Announcement callbacks ────────────────────────────────────────────────

  if (data.startsWith('ann_confirm_')) {
    const gChatId = data.replace('ann_confirm_', '');
    const pending = await getPending(env, userId);
    if (!pending || pending.step !== 'confirm_ann') return;
    if (!await isAuthorized(env, gChatId, userId)) {
      await setPending(env, userId, null);
      await editMsg(env, chatId, msgId, 'أنت لست مشرفاً في هذه المجموعة بعد الآن.');
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
        id: sentMsg.message_id,
        text: (pending.annText || '').substring(0, 80),
        hasPhoto: !!pending.photoFileId,
        date: new Date().toISOString(),
      });
      await saveAnnouncements(env, gChatId, anns);
      await setPending(env, userId, null);
      await editMsg(env, chatId, msgId,
        '✅ تم إرسال الإعلان وتثبيته في المجموعة بنجاح!',
        { inline_keyboard: [
          [{ text: '📌 إعلان جديد', callback_data: 'announce_' + gChatId }],
          [{ text: '🗑️ إدارة الإعلانات', callback_data: 'ann_list_' + gChatId }],
          [{ text: '🔙 رجوع للقائمة', callback_data: 'menu_' + gChatId }],
        ]}
      );
    } else {
      await setPending(env, userId, null);
      await editMsg(env, chatId, msgId,
        '❌ حدث خطأ أثناء إرسال الإعلان. تأكد أن البوت مشرف في المجموعة.',
        { inline_keyboard: [[{ text: '🔙 رجوع للقائمة', callback_data: 'menu_' + gChatId }]] }
      );
    }
    return;
  }

  if (data.startsWith('ann_cancel_')) {
    const gChatId = data.replace('ann_cancel_', '');
    await setPending(env, userId, null);
    await editMsg(env, chatId, msgId, '❌ تم إلغاء الإعلان.',
      { inline_keyboard: [[{ text: '🔙 رجوع للقائمة', callback_data: 'menu_' + gChatId }]] }
    );
    return;
  }

  if (data.startsWith('ann_list_')) {
    const gChatId = data.replace('ann_list_', '');
    if (!await isAuthorized(env, gChatId, userId)) return;
    await showAnnList(env, chatId, msgId, gChatId);
    return;
  }

  if (data.startsWith('ann_del_')) {
    const parts = data.replace('ann_del_', '').split('_');
    const idx = parseInt(parts[0]);
    const gChatId = parts.slice(1).join('_');
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
    await setPending(env, userId, {
      step: 'await_ann_content',
      gChatId,
      orig_msg_id: msgId,
      orig_chat_id: chatId,
    });
    await editMsg(env, chatId, msgId,
      '📌 تثبيت إعلان جديد\n\nأرسل نص الإعلان الآن، أو أرسل صورة مع نص (كـ caption).\n\nسيتم عرض معاينة للتأكيد قبل الإرسال.',
      { inline_keyboard: [[{ text: '❌ إلغاء', callback_data: 'ann_cancel_' + gChatId }]] }
    );
    return;
  }

  // ── My groups ─────────────────────────────────────────────────────────────

  if (data === 'my_groups') {
    const groups = await getUserGroups(env, userId);
    const keys = Object.keys(groups);
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

  if (data.startsWith('grp_')) {
    const gId = data.replace('grp_', '');
    if (!await isAuthorized(env, gId, userId)) {
      await editMsg(env, chatId, msgId, 'أنت لست مشرفاً في هذه المجموعة');
      return;
    }
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
    await editMsg(env, chatId, msgId,
      '✏️ تعديل رسالة الترحيب\n\nأرسل نص رسالة الترحيب الجديدة:\n\n💡 استخدم {name} لاسم العضو\n💡 استخدم {group} لاسم المجموعة\n\nمثال: اهلاً {name} في {group} 🎉',
      { inline_keyboard: [[{ text: '❌ إلغاء', callback_data: 'welcome_' + gChatId }]] }
    );
    return;
  }

  // ── تعديل وقت الوضع الليلي ───────────────────────────────────────────────
  if (data === 'edit_night_time_' + gChatId) {
    await setPending(env, userId, { step: 'await_night_start', gChatId });
    await editMsg(env, chatId, msgId,
      '🌙 تعديل وقت الوضع الليلي\n\nأرسل ساعة البداية (0-23):\n\nمثال: 23 (تعني 11 مساءً)',
      { inline_keyboard: [[{ text: '❌ إلغاء', callback_data: 'night_' + gChatId }]] }
    );
    return;
  }

  // ── إضافة مشرف فرعي ──────────────────────────────────────────────────────
  if (data === 'add_subadmin_' + gChatId) {
    const ownerCheck = await isOwner(env, gChatId, userId);
    if (!ownerCheck) {
      await editMsg(env, chatId, msgId, '❌ هذه الخاصية للمالك فقط.',
        { inline_keyboard: [[{ text: '🔙 رجوع', callback_data: 'subadmins_' + gChatId }]] }
      );
      return;
    }
    const groups = await getUserGroups(env, userId);
    const groupName = groups[gChatId.toString()] || gChatId;
    await setPending(env, userId, { step: 'await_sub_admin_id', gChatId, groupName });
    await editMsg(env, chatId, msgId,
      '👮 إضافة مشرف فرعي\n\nأرسل الـ ID الرقمي للمستخدم الذي تريد تعيينه مشرفاً فرعياً:\n\n💡 يمكن للمستخدم معرفة ID عبر بوت @userinfobot',
      { inline_keyboard: [[{ text: '❌ إلغاء', callback_data: 'subadmins_' + gChatId }]] }
    );
    return;
  }

  // ── حذف مشرف فرعي ────────────────────────────────────────────────────────
  if (data.startsWith('del_subadmin_')) {
    const withoutPrefix = data.replace('del_subadmin_', '');
    const firstUnder = withoutPrefix.indexOf('_');
    const subIdx = parseInt(withoutPrefix.substring(0, firstUnder));
    const saGChatId = withoutPrefix.substring(firstUnder + 1);
    const ownerCheck = await isOwner(env, saGChatId, userId);
    if (!ownerCheck) return;
    const saSettings = await getSettings(env, saGChatId);
    if (!isNaN(subIdx) && subIdx >= 0 && subIdx < saSettings.sub_admins.length) {
      saSettings.sub_admins.splice(subIdx, 1);
      await saveSettings(env, saGChatId, saSettings);
    }
    await showSubAdminsMenu(env, chatId, msgId, saGChatId, saSettings, userId);
    return;
  }

  // ── كتم بمدة محددة (على مستخدم معين) ────────────────────────────────────
  if (data.startsWith('mute_dur_')) {
    const parts = data.replace('mute_dur_', '').split('_');
    const durSecs = parseInt(parts[0]);
    const targetId = parseInt(parts[1]);
    const mGChatId = parts.slice(2).join('_');
    if (!await isAuthorized(env, mGChatId, userId)) return;
    await restrictUser(env, parseInt(mGChatId), targetId, false, durSecs);
    const labels = { 600: '10 دقائق', 1800: '30 دقيقة', 3600: 'ساعة واحدة', 43200: '12 ساعة', 86400: '24 ساعة' };
    const label = labels[durSecs] || (durSecs / 60 + ' دقيقة');
    await editMsg(env, chatId, msgId,
      '🔇 تم كتم المستخدم لمدة ' + label + ' ✅',
      { inline_keyboard: [[{ text: '🔙 رجوع للقائمة', callback_data: 'menu_' + mGChatId }]] }
    );
    return;
  }

  // ── تعيين مدة الكتم الافتراضية ───────────────────────────────────────────
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

  // ── وضع الكتمان المؤقت (Slow Mode) ───────────────────────────────────────
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

  // ── Toggles ───────────────────────────────────────────────────────────────

  const toggles = {
    ['twelcome_' + gChatId]: () => { settings.welcome_enabled = !settings.welcome_enabled; return () => showWelcomeMenu(env, chatId, msgId, gChatId, settings); },
    ['tcaptcha_' + gChatId]: () => { settings.captcha_enabled = !settings.captcha_enabled; return () => showWelcomeMenu(env, chatId, msgId, gChatId, settings); },
    ['tlinks_' + gChatId]: () => { settings.links_locked = !settings.links_locked; return () => showProtectMenu(env, chatId, msgId, gChatId, settings); },
    ['tforward_' + gChatId]: () => { settings.anti_forward = !settings.anti_forward; return () => showProtectMenu(env, chatId, msgId, gChatId, settings); },
    ['tchannel_' + gChatId]: () => { settings.anti_channel = !settings.anti_channel; return () => showProtectMenu(env, chatId, msgId, gChatId, settings); },
    ['twarns_' + gChatId]: () => { settings.warnings_enabled = !settings.warnings_enabled; return () => showWarnsMenu(env, chatId, msgId, gChatId, settings); },
    ['tnight_' + gChatId]: () => { settings.night_mode.enabled = !settings.night_mode.enabled; return () => showNightMenu(env, chatId, msgId, gChatId, settings); },
    ['tphoto_' + gChatId]: () => { settings.media_lock.photo = !settings.media_lock.photo; return async () => { await applyMediaPermissions(env, parseInt(gChatId), settings); await showMediaMenu(env, chatId, msgId, gChatId, settings); }; },
    ['tvideo_' + gChatId]: () => { settings.media_lock.video = !settings.media_lock.video; return async () => { await applyMediaPermissions(env, parseInt(gChatId), settings); await showMediaMenu(env, chatId, msgId, gChatId, settings); }; },
    ['tsticker_' + gChatId]: () => { settings.media_lock.sticker = !settings.media_lock.sticker; return async () => { await applyMediaPermissions(env, parseInt(gChatId), settings); await showMediaMenu(env, chatId, msgId, gChatId, settings); }; },
    ['taudio_' + gChatId]: () => { settings.media_lock.audio = !settings.media_lock.audio; return async () => { await applyMediaPermissions(env, parseInt(gChatId), settings); await showMediaMenu(env, chatId, msgId, gChatId, settings); }; },
    ['tgif_' + gChatId]: () => { settings.media_lock.gif = !settings.media_lock.gif; return async () => { await applyMediaPermissions(env, parseInt(gChatId), settings); await showMediaMenu(env, chatId, msgId, gChatId, settings); }; },
    ['tdoc_' + gChatId]: () => { settings.media_lock.document = !settings.media_lock.document; return async () => { await applyMediaPermissions(env, parseInt(gChatId), settings); await showMediaMenu(env, chatId, msgId, gChatId, settings); }; },
  };

  if (data.startsWith('wmax_')) {
    const parts2 = data.split('_');
    const num = parseInt(parts2[1]);
    settings.max_warnings = num;
    await saveSettings(env, gChatId, settings);
    await showWarnsMenu(env, chatId, msgId, gChatId, settings);
    return;
  }

  if (data.startsWith('wact_')) {
    const action = data.split('_')[1];
    settings.warn_action = action;
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
      [{ text: '👮 المشرفون الفرعيون', callback_data: 'subadmins_' + gChatId }],
      [{ text: '📌 تثبيت إعلان', callback_data: 'announce_' + gChatId }],
      [{ text: '🔙 رجوع لمجموعاتي', callback_data: 'my_groups' }],
    ]}
  );
}

async function showAnnList(env, chatId, msgId, gChatId, notice) {
  const anns = await getAnnouncements(env, gChatId);
  let bodyText = '📌 الإعلانات المثبتة الحالية\n\n';
  if (notice) bodyText = notice + '\n\n' + bodyText;
  const btns = [];
  if (anns.length === 0) {
    bodyText += 'لا توجد إعلانات مسجلة حتى الآن.';
  } else {
    anns.forEach((a, i) => {
      const icon = a.hasPhoto ? '🖼️' : '📝';
      const preview = a.text ? a.text.substring(0, 40) + (a.text.length > 40 ? '...' : '') : '(بدون نص)';
      bodyText += (i + 1) + '. ' + icon + ' ' + preview + '\n';
      btns.push([{ text: '🗑️ حذف الإعلان ' + (i + 1), callback_data: 'ann_del_' + i + '_' + gChatId }]);
    });
  }
  btns.push([{ text: '📌 إضافة إعلان جديد', callback_data: 'announce_' + gChatId }]);
  btns.push([{ text: '🔙 رجوع للقائمة', callback_data: 'menu_' + gChatId }]);
  await editMsg(env, chatId, msgId, bodyText, { inline_keyboard: btns });
}

function on(v) { return v ? '✅' : '❌'; }

async function showWelcomeMenu(env, chatId, msgId, gChatId, s) {
  await editMsg(env, chatId, msgId,
    '👋 الترحيب والكابتشا\n\nالترحيب: ' + on(s.welcome_enabled) + '\nالكابتشا: ' + on(s.captcha_enabled) + '\n\nرسالة الترحيب الحالية:\n' + esc(s.welcome_message),
    { inline_keyboard: [
      [{ text: on(s.welcome_enabled) + ' الترحيب', callback_data: 'twelcome_' + gChatId }, { text: on(s.captcha_enabled) + ' الكابتشا', callback_data: 'tcaptcha_' + gChatId }],
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
      [{ text: on(s.media_lock.gif) + ' GIF', callback_data: 'tgif_' + gChatId }, { text: on(s.media_lock.document) + ' ملفات', callback_data: 'tdoc_' + gChatId }],
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
  const list = Object.keys(s.auto_replies).length > 0
    ? Object.entries(s.auto_replies).map(([k, v]) => k + ': ' + v).join('\n')
    : 'لا توجد ردود تلقائية';
  await editMsg(env, chatId, msgId,
    '🤖 الردود التلقائية\n\n' + list + '\n\nلإضافة رد أرسل في المجموعة:\n/addreply كلمة الرد\n\nلحذف رد:\n/removereply كلمة',
    { inline_keyboard: [[{ text: '🔙 رجوع', callback_data: 'menu_' + gChatId }]] }
  );
}

async function showWordsMenu(env, chatId, msgId, gChatId, s) {
  const btns = [];
  if (s.banned_words.length > 0) {
    s.banned_words.forEach((word, idx) => {
      btns.push([{ text: '🗑️ حذف: ' + word, callback_data: 'del_word_' + idx + '_' + gChatId }]);
    });
  }
  const wordsList = s.banned_words.length > 0
    ? s.banned_words.map((w, i) => (i + 1) + '. ' + w).join('\n')
    : 'لا توجد كلمات محظورة';
  btns.push([{ text: '➕ إضافة كلمة محظورة', callback_data: 'add_word_' + gChatId }]);
  btns.push([{ text: '🔙 رجوع', callback_data: 'menu_' + gChatId }]);
  await editMsg(env, chatId, msgId,
    '🚫 الكلمات المحظورة\n\n' + wordsList,
    { inline_keyboard: btns }
  );
}

async function showMuteMenu(env, chatId, msgId, gChatId, s) {
  const settings = s || await getSettings(env, gChatId);
  const cur = settings.default_mute_duration || 3600;
  const labels = { 600: '10 دقائق', 1800: '30 دقيقة', 3600: 'ساعة واحدة', 43200: '12 ساعة', 86400: '24 ساعة' };
  const sel = (v) => cur === v ? '◀ ' + labels[v] + ' ▶' : labels[v];
  await editMsg(env, chatId, msgId,
    '🔇 إعدادات الكتم\n\nالمدة الافتراضية الحالية: ' + (labels[cur] || (cur / 60 + ' دقيقة')) + '\n\nاختر مدة الكتم الافتراضية عند استخدام أمر /mute بدون تحديد مدة\n\nيمكنك أيضاً كتم بمدة مخصصة بكتابة: /mute 45 (بالدقائق)',
    { inline_keyboard: [
      [
        { text: sel(600), callback_data: 'mute_def_600_' + gChatId },
        { text: sel(1800), callback_data: 'mute_def_1800_' + gChatId },
      ],
      [
        { text: sel(3600), callback_data: 'mute_def_3600_' + gChatId },
        { text: sel(43200), callback_data: 'mute_def_43200_' + gChatId },
      ],
      [
        { text: sel(86400), callback_data: 'mute_def_86400_' + gChatId },
      ],
      [{ text: '🔙 رجوع', callback_data: 'menu_' + gChatId }],
    ]}
  );
}

async function showSlowMenu(env, chatId, msgId, gChatId, s) {
  const settings = s || await getSettings(env, gChatId);
  const cur = settings.slow_mode_delay || 0;
  const opts = [
    { label: '❌ إيقاف', val: 0 },
    { label: '10 ثوانٍ', val: 10 },
    { label: '30 ثانية', val: 30 },
    { label: 'دقيقة واحدة', val: 60 },
    { label: '5 دقائق', val: 300 },
    { label: '10 دقائق', val: 600 },
    { label: 'ساعة', val: 3600 },
  ];
  const sel = (v, l) => cur === v ? '◀ ' + l + ' ▶' : l;
  const rows = [];
  for (let i = 0; i < opts.length; i += 2) {
    const row = [{ text: sel(opts[i].val, opts[i].label), callback_data: 'slow_set_' + opts[i].val + '_' + gChatId }];
    if (opts[i + 1]) row.push({ text: sel(opts[i + 1].val, opts[i + 1].label), callback_data: 'slow_set_' + opts[i + 1].val + '_' + gChatId });
    rows.push(row);
  }
  rows.push([{ text: '🔙 رجوع', callback_data: 'menu_' + gChatId }]);
  const statusLabel = cur === 0 ? '❌ متوقف' : (opts.find(o => o.val === cur)?.label || cur + ' ثانية');
  await editMsg(env, chatId, msgId,
    '⏳ وضع الكتمان المؤقت\n\nيجبر الأعضاء على الانتظار بين كل رسالة وأخرى\nالعداد يظهر تلقائياً في شريط الكتابة لدى الأعضاء\n\nالحالة الحالية: ' + statusLabel,
    { inline_keyboard: rows }
  );
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
      if (ownerCheck) {
        btns.push([{ text: '🗑️ حذف المشرف الفرعي ' + (idx + 1), callback_data: 'del_subadmin_' + idx + '_' + gChatId }]);
      }
    });
  }
  if (ownerCheck) {
    bodyText += '\n💡 المشرف الفرعي يمكنه إدارة إعدادات البوت فقط (لا يملك صلاحيات مشرف تيليغرام)';
    btns.push([{ text: '➕ إضافة مشرف فرعي', callback_data: 'add_subadmin_' + gChatId }]);
  } else {
    bodyText += '\n⚠️ إضافة وحذف المشرفين الفرعيين للمالك فقط';
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
    if (settings.warn_action === 'ban') { await banUser(env, chatId, userId); return 'تم حظر ' + esc(firstName) + ' بعد ' + settings.max_warnings + ' إنذارات'; }
    if (settings.warn_action === 'kick') { await kickUser(env, chatId, userId); return 'تم طرد ' + esc(firstName) + ' بعد ' + settings.max_warnings + ' إنذارات'; }
    if (settings.warn_action === 'mute') {
      const muteDur = settings.default_mute_duration || 3600;
      const until = await restrictUser(env, chatId, userId, false, muteDur);
      await setMuteData(env, chatId, userId, until);
      const fmt = formatMuteExpiry(muteDur);
      await sendMsg(env, userId,
        '🔇 تم كتمك في المجموعة\n⏱ المدة: ' + fmt.label + '\n🕐 ينتهي الكتم الساعة: ' + fmt.time + '\n\nاضغط الزر أدناه لمعرفة الوقت المتبقي بدقة:',
        { inline_keyboard: [[{ text: '⏱ كم باقي؟', callback_data: 'check_mute_' + chatId + '_' + userId }]] }
      ).catch(() => {});
      return '🔇 تم كتم ' + esc(firstName) + ' لمدة ' + fmt.label + ' بعد ' + settings.max_warnings + ' إنذارات';
    }
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
  try {
    await env.USER_DATA.put('mute_' + chatId + '_' + userId, untilTimestamp.toString(), { expirationTtl: TTL });
  } catch {}
}

async function getMuteUntil(env, chatId, userId) {
  try {
    const d = await env.USER_DATA.get('mute_' + chatId + '_' + userId);
    return d ? parseInt(d) : null;
  } catch { return null; }
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
  const perms = {
    can_send_messages: canSend,
    can_send_media_messages: canSend,
    can_send_other_messages: canSend,
    can_add_web_page_previews: canSend,
    can_send_polls: canSend,
    can_invite_users: true,
    can_pin_messages: false,
    can_change_info: false,
  };
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
    can_send_voice_notes: !ml.audio,
    can_send_video_notes: !ml.video,
    can_send_other_messages: !noOther,
    can_add_web_page_previews: !settings.links_locked,
    can_send_polls: true,
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
