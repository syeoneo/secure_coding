(function () {
  "use strict";

  const page = document.querySelector(
    "[data-global-chat-page]"
  );

  if (!page) {
    return;
  }

  const currentUserId = String(
    page.dataset.currentUserId || ""
  );

  const profileUrlTemplate =
    page.dataset.profileUrlTemplate || "";

  const chatForm =
    document.getElementById("chat-form");

  const chatInput =
    document.getElementById("chat-input");

  const chatMessages =
    document.getElementById("chat-messages");

  const chatError =
    document.getElementById("chat-error");

  const chatLength =
    document.getElementById("chat-length");

  const chatSendButton =
    document.getElementById(
      "chat-send-button"
    );

  if (
    !currentUserId ||
    !chatForm ||
    !chatInput ||
    !chatMessages ||
    !chatError ||
    !chatLength ||
    !chatSendButton
  ) {
    console.error(
      "전체 채팅에 필요한 화면 요소를 찾지 못했습니다."
    );

    return;
  }

  const pendingReadMessageIds =
    new Set();

  const blockedUserIds =
    new Set();

  let socket = null;

  function formatMessageTime(value) {
    const date = new Date(value);

    if (Number.isNaN(date.getTime())) {
      return "";
    }

    return date.toLocaleTimeString(
      "ko-KR",
      {
        hour: "2-digit",
        minute: "2-digit",
        hour12: false
      }
    );
  }

  function makeProfileUrl(userId) {
    return profileUrlTemplate.replace(
      "__USER_ID__",
      encodeURIComponent(userId)
    );
  }

  function updateComposerState() {
    const messageLength =
      chatInput.value.length;

    const trimmedMessage =
      chatInput.value.trim();

    const isOverLimit =
      messageLength > 500;

    chatLength.textContent =
      `${messageLength} / 500`;

    chatLength.classList.toggle(
      "over-limit",
      isOverLimit
    );

    if (isOverLimit) {
      chatError.textContent =
        "메시지는 최대 500자까지 전송할 수 있습니다.";
    } else if (
      chatError.dataset.connectionError !==
      "true"
    ) {
      chatError.textContent = "";
    }

    chatSendButton.disabled =
      trimmedMessage.length === 0 ||
      isOverLimit;
  }

  function createMessageElement(data) {
    const senderId = String(
      data.sender_id || ""
    );

    const messageId = String(
      data.message_id || ""
    );

    const isOwnMessage =
      senderId === currentUserId;

    const messageRow =
      document.createElement("div");

    messageRow.className =
      `message-row ${
        isOwnMessage ? "own" : "other"
      }`;

    messageRow.dataset.senderId =
      senderId;

    const messageContent =
      document.createElement("div");

    messageContent.className =
      "message-content";

    if (!isOwnMessage) {
      const usernameLink =
        document.createElement("a");

      usernameLink.className =
        "message-username";

      usernameLink.href =
        makeProfileUrl(senderId);

      usernameLink.textContent =
        data.username || "사용자";

      usernameLink.title =
        "프로필 보기";

      messageContent.appendChild(
        usernameLink
      );
    }

    const messageBubble =
      document.createElement("div");

    messageBubble.className =
      "message-bubble";

    /*
     * 사용자가 입력한 내용을 HTML로
     * 실행하지 않고 일반 텍스트로 표시한다.
     */
    messageBubble.textContent =
      String(data.message || "");

    const messageMeta =
      document.createElement("div");

    messageMeta.className =
      "message-meta";

    const sentTime =
      formatMessageTime(
        data.created_at
      );

    if (isOwnMessage) {
      messageMeta.id =
        `message-status-${messageId}`;

      if (
        pendingReadMessageIds.has(
          messageId
        )
      ) {
        messageMeta.textContent =
          `읽음 · ${sentTime}`;

        pendingReadMessageIds.delete(
          messageId
        );
      } else {
        messageMeta.textContent =
          `안읽음 · ${sentTime}`;
      }
    } else {
      messageMeta.textContent =
        sentTime;
    }

    messageContent.appendChild(
      messageBubble
    );

    messageContent.appendChild(
      messageMeta
    );

    messageRow.appendChild(
      messageContent
    );

    return {
      element: messageRow,
      isOwnMessage: isOwnMessage
    };
  }

  function removeBlockedUserMessages(
    blockedUserId
  ) {
    document.querySelectorAll(
      ".message-row"
    ).forEach(
      function (messageRow) {
        if (
          String(
            messageRow.dataset.senderId ||
            ""
          ) === blockedUserId
        ) {
          messageRow.remove();
        }
      }
    );
  }

  function appendChatNotice(message) {
    const notice =
      document.createElement("div");

    notice.className =
      "chat-notice";

    notice.textContent =
      message;

    chatMessages.appendChild(
      notice
    );

    chatMessages.scrollTop =
      chatMessages.scrollHeight;
  }

  /*
   * Socket.IO 연결 전에도 입력창의
   * 글자 수와 버튼 상태는 동작한다.
   */
  chatInput.addEventListener(
    "input",
    updateComposerState
  );

  chatInput.addEventListener(
    "keydown",
    function (event) {
      if (
        event.key === "Enter" &&
        !event.shiftKey
      ) {
        event.preventDefault();

        if (
          !chatSendButton.disabled
        ) {
          chatForm.requestSubmit();
        }
      }
    }
  );

  chatForm.addEventListener(
    "submit",
    function (event) {
      event.preventDefault();

      const message =
        chatInput.value.trim();

      if (!message) {
        chatError.textContent =
          "메시지를 입력해주세요.";

        return;
      }

      if (message.length > 500) {
        chatError.textContent =
          "메시지는 최대 500자까지 전송할 수 있습니다.";

        return;
      }

      if (
        !socket ||
        !socket.connected
      ) {
        chatError.textContent =
          "채팅 서버에 연결되지 않았습니다. 잠시 후 다시 시도해주세요.";

        chatError.dataset.connectionError =
          "true";

        return;
      }

      socket.emit(
        "send_message",
        {
          message: message
        }
      );

      chatInput.value = "";

      chatError.dataset.connectionError =
        "false";

      updateComposerState();

      chatInput.focus();
    }
  );

  updateComposerState();

  chatMessages.scrollTop =
    chatMessages.scrollHeight;

  if (
    typeof window.io !== "function"
  ) {
    chatError.textContent =
      "채팅 모듈을 불러오지 못했습니다.";

    chatError.dataset.connectionError =
      "true";

    return;
  }

  socket = window.io();

  socket.on(
    "connect",
    function () {
      chatError.dataset.connectionError =
        "false";

      chatError.textContent = "";

      updateComposerState();
    }
  );

  socket.on(
    "disconnect",
    function () {
      chatError.textContent =
        "채팅 서버와의 연결이 끊어졌습니다.";

      chatError.dataset.connectionError =
        "true";
    }
  );

  socket.on(
    "connect_error",
    function () {
      chatError.textContent =
        "채팅 서버에 연결하지 못했습니다.";

      chatError.dataset.connectionError =
        "true";
    }
  );

  socket.on(
    "message",
    function (data) {
      if (
        !data ||
        typeof data.message !==
          "string" ||
        typeof data.message_id !==
          "string"
      ) {
        return;
      }

      const senderId = String(
        data.sender_id || ""
      );

      /*
       * 차단 관계인 사용자가 보낸
       * 전체 채팅 메시지는 표시하지 않는다.
       */
      if (
        blockedUserIds.has(
          senderId
        )
      ) {
        return;
      }

      const result =
        createMessageElement(data);

      chatMessages.appendChild(
        result.element
      );

      chatMessages.scrollTop =
        chatMessages.scrollHeight;

      if (!result.isOwnMessage) {
        socket.emit(
          "message_read",
          {
            message_id:
              data.message_id
          }
        );
      }
    }
  );

  socket.on(
    "message_read",
    function (data) {
      if (
        !data ||
        typeof data.message_id !==
          "string"
      ) {
        return;
      }

      const statusElement =
        document.getElementById(
          `message-status-${data.message_id}`
        );

      if (statusElement) {
        const parts =
          statusElement.textContent.split(
            "·"
          );

        const sentTime =
          parts.length > 1
            ? parts[1].trim()
            : "";

        statusElement.textContent =
          `읽음 · ${sentTime}`;
      } else {
        pendingReadMessageIds.add(
          data.message_id
        );
      }
    }
  );

  /*
   * 사용자를 차단하거나 차단을
   * 해제했을 때 서버에서 전달받는다.
   */
  socket.on(
    "global_block_updated",
    function (data) {
      if (
        !data ||
        !data.other_user_id
      ) {
        return;
      }

      const otherUserId = String(
        data.other_user_id
      );

      if (
        data.blocked === false
      ) {
        blockedUserIds.delete(
          otherUserId
        );

        appendChatNotice(
          "사용자 차단이 해제되었습니다."
        );

        return;
      }

      blockedUserIds.add(
        otherUserId
      );

      removeBlockedUserMessages(
        otherUserId
      );

      appendChatNotice(
        "차단한 사용자와의 메시지가 숨겨졌습니다."
      );
    }
  );

  socket.on(
    "chat_error",
    function (data) {
      chatError.textContent =
        data && data.message
          ? data.message
          : "메시지를 전송할 수 없습니다.";
    }
  );
})();